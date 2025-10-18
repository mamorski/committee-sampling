import argparse
import json
import math
import os
import random
import signal
import sys
import tarfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import rmtree
from subprocess import Popen, STDOUT
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm


@dataclass
class Paths:
    base_dir: Path
    bin_dir: Path
    logs_dir: Path
    configs_dir: Path
    pids_file: Path
    bootstrap_log: Path
    bootstrap_pid_file: Path
    bootstrap_address_file: Path


@dataclass
class Processes:
    server_proc: Optional[Popen]
    node_procs: List[Popen]


@dataclass(frozen=True)
class AdversarySettings:
    seed: Optional[int]
    drop_probability: float
    jitter_min: str
    jitter_max: str
    clock_skew: str
    ex_ante_equivocator: bool
    freshness_enabled: bool
    freshness_mode: str
    freshness_stale_rounds: int
    freshness_truncate_leaf: bool


def percent_to_count(total: int, percent: float) -> int:
    if percent <= 0 or total <= 0:
        return 0
    return max(1, math.ceil(total * (percent / 100.0)))


def _ensure_duration(value: Optional[str]) -> str:
    if not value:
        return "0s"
    return str(value)


def build_adversary_dict(enabled: bool, settings: AdversarySettings) -> Dict[str, object]:
    config: Dict[str, object] = {
        "enabled": bool(enabled),
        "drop_probability": settings.drop_probability,
        "jitter_min": _ensure_duration(settings.jitter_min),
        "jitter_max": _ensure_duration(settings.jitter_max),
        "clock_skew": _ensure_duration(settings.clock_skew),
        "ex_ante": {"equivocator": settings.ex_ante_equivocator},
        "ex_post": {
            "freshness_cheater": {
                "enabled": settings.freshness_enabled,
                "mode": settings.freshness_mode,
                "stale_rounds": settings.freshness_stale_rounds,
                "truncate_leaf": settings.freshness_truncate_leaf,
            }
        },
    }
    if settings.seed is not None:
        config["seed"] = int(settings.seed)
    return config


def kill_random_node(
        procs: Processes,
        stop_event: threading.Event,
        kill_random_up_to: int,
        kill_random_delay_sec: int,
        kill_probability: float,
) -> None:
    if len(procs.node_procs) == 0:
        return

    killed_procs = []
    while not stop_event.is_set():
        if random.random() < kill_probability:
            while True:
                random_node = random.choice(procs.node_procs)
                if random_node.poll() is None and random_node not in killed_procs:
                    break

            random_node.terminate()
            killed_procs.append(random_node)

        if len(killed_procs) >= kill_random_up_to:
            return
        time.sleep(kill_random_delay_sec)


def validate_args(
        num_nodes: int,
        max_outbound_degree: int,
        diameter: int,
        log_level: str,
        drop_on_send_percent: float,
        drop_on_send_probability: float,
        adversary_percent: float,
        adversary_settings: AdversarySettings,
        degree_slack: int,
) -> None:
    if num_nodes < 2:
        sys.exit("Error: Number of nodes must be a positive integer >= 2")
    if max_outbound_degree < 1:
        sys.exit("Error: Max outbound degree must be a positive integer >= 1")
    if diameter < 2:
        sys.exit("Error: Diameter must be a positive integer >= 2")
    if log_level not in {"debug", "info", "warn", "error"}:
        sys.exit("Error: Log level must be one of: debug, info, warn, error")
    if not 0 <= drop_on_send_percent <= 100:
        sys.exit("Error: drop-on-send percent must be within [0, 100]")
    if not 0.0 <= drop_on_send_probability <= 1.0:
        sys.exit("Error: drop-on-send probability must be within [0.0, 1.0]")
    if not 0 <= adversary_percent <= 100:
        sys.exit("Error: adversary percent must be within [0, 100]")
    if not 0.0 <= adversary_settings.drop_probability <= 1.0:
        sys.exit("Error: adversary drop_probability must be within [0.0, 1.0]")
    if adversary_settings.freshness_mode.lower() not in {"stale", "truncate", "both"}:
        sys.exit("Error: adversary freshness mode must be one of: stale, truncate, both")
    if adversary_settings.freshness_stale_rounds < 1:
        sys.exit("Error: adversary freshness stale_rounds must be >= 1")
    if degree_slack < 0:
        sys.exit("Error: degree slack must be >= 0")


def ensure_files(paths: Paths) -> None:
    if not paths.bin_dir.joinpath("server").is_file():
        sys.exit(f"Error: Server binary not found at {paths.bin_dir / 'server'}")
    if not paths.bin_dir.joinpath("committee-sampling").is_file():
        sys.exit(
            f"Error: Committee-sampling binary not found at {paths.bin_dir / 'committee-sampling'}"
        )

    # Ensure configs directory exists
    paths.configs_dir.mkdir(parents=True, exist_ok=True)

    # Create a default dev-server.json if it does not exist
    dev_server_cfg = paths.configs_dir / "dev-server.json"
    if not dev_server_cfg.is_file():
        default_server_cfg = {
            "network": {"listen_address": "0.0.0.0", "listen_port": 4001},
            "logging": {"level": "INFO"},
        }
        with dev_server_cfg.open("w", encoding="utf-8") as f:
            json.dump(default_server_cfg, f, indent=2)
        print(f"Created default bootstrap server config at: {dev_server_cfg}")


def mkdirs(paths: Paths) -> None:
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    paths.configs_dir.mkdir(parents=True, exist_ok=True)
    # Clear previous PIDs file
    paths.pids_file.write_text("")


def write_committee_config(
        paths: Paths,
        session_id: str,
        max_outbound_degree: int,
        diameter: int,
        graph_building_rounds: int,
        num_nodes: int,
        bootstrap_address: str,
        log_level: str,
        verify_timeout: str,
        graph_discovery_timeout: str,
        graph_building_round_timeout: str,
        config_file_path: Optional[Path] = None,
        metrics_enabled: bool = False,
        pushgateway_enabled: bool = False,
        drop_on_send_enabled: bool = False,
        drop_on_send_probability: float = 0.0,
        committee_size: int = 30,
        adversary_enabled: bool = False,
        adversary_settings: Optional[AdversarySettings] = None,
        degree_slack: int = 0,
) -> Path:
    config_file = config_file_path or (
            paths.configs_dir / "committee-sampling-conf.json"
    )

    settings = adversary_settings or AdversarySettings(
        seed=None,
        drop_probability=0.0,
        jitter_min="0s",
        jitter_max="0s",
        clock_skew="0s",
        ex_ante_equivocator=False,
        freshness_enabled=False,
        freshness_mode="stale",
        freshness_stale_rounds=1,
        freshness_truncate_leaf=False,
    )

    config = {
        "network": {
            "listen_port": 0,
            "max_outbound_degree": max_outbound_degree,
            "degree_slack": degree_slack,
            "discovery_config": {
                "protocol_id": "/committee-sampling/1.0.0",
                "interval": "5s",
                "bootstrap_peers": [bootstrap_address],
            },
            "drop_on_send": drop_on_send_enabled,
            "drop_on_send_probability": drop_on_send_probability,
            "adversary": build_adversary_dict(adversary_enabled, settings),
        },
        "graph": {
            "diameter": diameter,
            "grading_levels": 5,
            "building_rounds": graph_building_rounds,
        },
        "committee": {
            "session_id": session_id,
            "lambda": 256,
            "weight": 1,
            "delta_w": 10,
            "committee_size": committee_size,
            "delay": 20,
            "total_weight": num_nodes,
        },
        "synchronization": {
            "type": 0,
            "ex_ante_round_timeout": verify_timeout,
            "ex_post_round_timeout": verify_timeout,
            "mdag_round_timeout": "10s",
            "start_time": int(time.time()) + 60,
            "graph_discovery_timeout": _ensure_duration(graph_discovery_timeout),
            "graph_building_round_timeout": _ensure_duration(graph_building_round_timeout),
            "time_server": "time.google.com",
        },
        "logger": {"level": log_level},
        "metrics": {
            "enabled": metrics_enabled,
            "push_gateway": {
                "enabled": pushgateway_enabled,
                "url": "http://localhost:9091",
            },
            "http_server": {"enabled": False, "port": 0, "path": "/metrics"},
            "push_interval": "30s",
            "job_name": "committee-sampling-simulation",
        },
    }

    with config_file.open("w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

    return config_file


def start_bootstrap_server(paths: Paths) -> Popen:
    print("Starting DHT bootstrap server...")
    server_bin = str(paths.bin_dir / "server")
    server_cfg = str(paths.configs_dir / "dev-server.json")
    # Ensure parent dir exists for bootstrap log (it may live inside logs_dir per run)
    paths.bootstrap_log.parent.mkdir(parents=True, exist_ok=True)
    # Open, spawn, then close our handle to avoid FD leaks in the parent
    with paths.bootstrap_log.open("w") as bootstrap_log_fh:
        proc = Popen(
            [server_bin, "-config", server_cfg], stdout=bootstrap_log_fh, stderr=STDOUT
        )
    paths.bootstrap_pid_file.write_text(str(proc.pid))
    print(f"Bootstrap server started (PID: {proc.pid})")
    print("Waiting 10 seconds for bootstrap server to be ready...")
    time.sleep(10)
    return proc


def read_bootstrap_address(paths: Paths) -> str:
    if not paths.bootstrap_address_file.is_file():
        print("Failed to read bootstrap address from file")
        print(
            "Check bootstrap.log for the bootstrap address and update the script manually"
        )
        sys.exit(1)
    address = paths.bootstrap_address_file.read_text().strip()
    print(f"Bootstrap address: {address}")
    return address


def start_node(paths: Paths, node_id: int, config_file: Path) -> Popen:
    log_file = paths.logs_dir / f"node-{node_id}.log"
    cmd = [str(paths.bin_dir / "committee-sampling"), "-config", str(config_file)]
    # Open, spawn, then close our handle to avoid FD leaks in the parent
    with log_file.open("w") as log_fh:
        proc = Popen(cmd, stdout=log_fh, stderr=STDOUT)
    with paths.pids_file.open("a") as pf:
        pf.write(f"{proc.pid}\n")

    return proc


def backup_logs(paths: Paths, session_id: str) -> Path:
    archive_path = paths.base_dir / f"{session_id}.tar.gz"
    if not paths.logs_dir.exists():
        return archive_path
    print(f"Archiving logs to: {archive_path}")
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(paths.logs_dir, arcname=session_id)
    return archive_path


# noinspection PyBroadException
def cleanup(paths: Paths, procs: Processes, session_id: str) -> None:
    print("")
    print("Stopping all nodes...")

    # Stop node processes
    for p in procs.node_procs:
        if p.poll() is None:
            try:
                p.terminate()
            except Exception:
                pass

    # Stop bootstrap server
    if paths.bootstrap_pid_file.is_file():
        try:
            boot_pid_str = paths.bootstrap_pid_file.read_text().strip()
            if boot_pid_str:
                os.kill(int(boot_pid_str), signal.SIGTERM)
        except Exception:
            pass
        try:
            paths.bootstrap_pid_file.unlink(missing_ok=True)
        except Exception:
            pass

    # Backup logs only
    archive_path = backup_logs(paths, session_id)

    # Clean up temporary logs (but keep configs as requested in bash script)
    print("Cleaning up temporary logs...")
    try:
        rmtree(paths.logs_dir, ignore_errors=True)
    except Exception:
        pass
    try:
        paths.pids_file.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        paths.bootstrap_address_file.unlink(missing_ok=True)
    except Exception:
        pass

    print("Cleaning up temporary configs...")
    try:
        rmtree(paths.configs_dir, ignore_errors=True)
    except Exception:
        pass

    print("All nodes and bootstrap server stopped")
    print(f"Logs archived: {archive_path}")
    print("Configuration files removed")


def run_one_simulation(
        base_paths: Paths,
        num_nodes: int,
        max_outbound_degree: int,
        diameter: int,
        log_level: str,
        run_label: str,
        drop_on_send_percent: float,
        drop_on_send_probability: float,
        adversary_percent: float,
        adversary_settings: AdversarySettings,
        degree_slack: int,
        kill_random_up_to: int,
        kill_random_delay_sec: int,
        kill_probability: float,
        verify_timeout: str = "30s",
        committee_size: int = 30,
        graph_building_rounds: int = 3,
        graph_discovery_timeout: str = "30s",
        graph_building_round_timeout: str = "2m",
) -> None:
    validate_args(
        num_nodes,
        max_outbound_degree,
        diameter,
        log_level,
        drop_on_send_percent,
        drop_on_send_probability,
        adversary_percent,
        adversary_settings,
        degree_slack,
    )

    # Create per-run paths (logs in a dedicated folder; bootstrap log inside logs folder)
    logs_dir = base_paths.base_dir / "logs" / run_label
    paths = Paths(
        base_dir=base_paths.base_dir,
        bin_dir=base_paths.bin_dir,
        logs_dir=logs_dir,
        configs_dir=base_paths.configs_dir,
        pids_file=base_paths.base_dir / f"node_pids_{run_label}.txt",
        bootstrap_log=logs_dir / "bootstrap.log",
        bootstrap_pid_file=base_paths.base_dir / f"bootstrap_pid_{run_label}.txt",
        bootstrap_address_file=base_paths.bootstrap_address_file,
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    session_id = f"simulation-{timestamp}-n{num_nodes}-m{max_outbound_degree}-d{diameter}-c{committee_size}"

    print(f"🚀 Starting {num_nodes} committee-sampling simulation nodes...")
    print(f"Session ID: {session_id}")
    print(f"Logs directory: {paths.logs_dir}")
    print(f"Configs directory: {paths.configs_dir}")
    print("")

    mkdirs(paths)
    ensure_files(paths)

    # Trap signals to clean up
    procs = Processes(server_proc=None, node_procs=[])
    stop_event = threading.Event()
    killer_thread = threading.Thread(
        target=kill_random_node,
        args=(
            procs,
            stop_event,
            kill_random_up_to,
            kill_random_delay_sec,
            kill_probability,
        ),
        daemon=True,
    )

    drop_count = percent_to_count(num_nodes, drop_on_send_percent)
    adversary_count = percent_to_count(num_nodes, adversary_percent)
    both_count = min(drop_count, adversary_count)
    drop_only_count = drop_count - both_count
    adversary_only_count = adversary_count - both_count
    base_count = num_nodes - (both_count + drop_only_count + adversary_only_count)
    if base_count < 0:
        sys.exit("Error: configuration assigns more specialised nodes than available")

    def _handle_signal(_, __):
        stop_event.set()
        cleanup(paths, procs, session_id)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Determine log level status
    log_level_status = f"{log_level} (default)"

    print("Network parameters:")
    print(f"- Number of nodes: {num_nodes}")
    print(f"- Max outbound degree: {max_outbound_degree}")
    print(f"- Diameter: {diameter}")
    print(f"- Graph building rounds: {graph_building_rounds}")
    print(f"- Graph discovery timeout: {graph_discovery_timeout}")
    print(f"- Graph building round timeout: {graph_building_round_timeout}")
    print(f"- Log level: {log_level_status}")
    print(f"- Drop-on-send nodes: {drop_count} ({drop_on_send_percent:.2f}%)")
    print(f"- Adversary-enabled nodes: {adversary_count} ({adversary_percent:.2f}%)")
    if adversary_count > 0:
        print("Adversary parameters:")
        print(f"  - Seed: {adversary_settings.seed if adversary_settings.seed is not None else 'derived'}")
        print(
            f"  - Network drop probability: {adversary_settings.drop_probability}")
        print(
            f"  - Network jitter: {adversary_settings.jitter_min} - {adversary_settings.jitter_max}")
        print(f"  - Clock skew: {adversary_settings.clock_skew}")
        print(
            f"  - Ex-Ante equivocator: {adversary_settings.ex_ante_equivocator}")
        freshness_status = "enabled" if adversary_settings.freshness_enabled else "disabled"
        print(
            f"  - Freshness cheater: {freshness_status} ({adversary_settings.freshness_mode})")
    if degree_slack > 0:
        print(f"- Degree slack: {degree_slack}")
    print("")

    # Start bootstrap server
    server_proc = start_bootstrap_server(paths)
    procs.server_proc = server_proc

    # Read bootstrap address
    bootstrap_address = read_bootstrap_address(paths)

    # Create configuration(s) only for distinct settings and reuse them
    print("")
    print("Starting nodes...")

    config_cache: Dict[Tuple[bool, bool], Path] = {}
    suffix_map: Dict[Tuple[bool, bool], str] = {
        (False, False): "base",
        (True, False): "drop",
        (False, True): "adv",
        (True, True): "drop-adv",
    }

    def ensure_config(drop_enabled: bool, adversary_enabled: bool) -> Path:
        key = (drop_enabled, adversary_enabled)
        if key in config_cache:
            return config_cache[key]
        suffix = suffix_map[key]
        conf_path = paths.configs_dir / f"committee-sampling-conf-{run_label}-{suffix}.json"
        write_committee_config(
            paths,
            session_id=session_id,
            max_outbound_degree=max_outbound_degree,
            diameter=diameter,
            graph_building_rounds=graph_building_rounds,
            num_nodes=num_nodes,
            bootstrap_address=bootstrap_address,
            log_level=log_level,
            config_file_path=conf_path,
            metrics_enabled=False,
            pushgateway_enabled=False,
            drop_on_send_enabled=drop_enabled,
            drop_on_send_probability=drop_on_send_probability if drop_enabled else 0.0,
            verify_timeout=verify_timeout,
            graph_discovery_timeout=graph_discovery_timeout,
            graph_building_round_timeout=graph_building_round_timeout,
            committee_size=committee_size,
            adversary_enabled=adversary_enabled,
            adversary_settings=adversary_settings,
            degree_slack=degree_slack,
        )
        config_cache[key] = conf_path
        return conf_path

    config_sequence: List[Path] = []
    if both_count > 0:
        cfg = ensure_config(True, True)
        config_sequence.extend([cfg] * both_count)
    if drop_only_count > 0:
        cfg = ensure_config(True, False)
        config_sequence.extend([cfg] * drop_only_count)
    if adversary_only_count > 0:
        cfg = ensure_config(False, True)
        config_sequence.extend([cfg] * adversary_only_count)
    if base_count > 0:
        cfg = ensure_config(False, False)
        config_sequence.extend([cfg] * base_count)

    if len(config_sequence) != num_nodes:
        sys.exit("Error: internal configuration mismatch while assigning node configs")

    for idx, cfg_path in enumerate(config_sequence, start=1):
        proc = start_node(paths, idx, cfg_path)
        procs.node_procs.append(proc)

    killer_thread.start()

    print("")
    print(f"All {num_nodes} nodes started successfully!")
    print("")
    print("Simulation Status:")
    print(f"- Nodes: {num_nodes}")
    print(f"- Max outbound degree: {max_outbound_degree} (provided)")
    print(f"- Diameter: {diameter} (provided)")
    print(f"- Log level: {log_level_status}")
    print("- Ports: Auto-assigned by system (port 0 configured)")
    print(f"- Session ID: {session_id}")
    print(f"- Drop-on-send nodes: {drop_count} ({drop_on_send_percent:.2f}%)")
    print(f"- Adversary-enabled nodes: {adversary_count} ({adversary_percent:.2f}%)")
    if both_count > 0:
        print(f"  - Nodes with both enabled: {both_count}")
    if drop_only_count > 0:
        print(f"  - Drop-only nodes: {drop_only_count}")
    if adversary_only_count > 0:
        print(f"  - Adversary-only nodes: {adversary_only_count}")
    if degree_slack > 0:
        print(f"- Degree slack: {degree_slack}")
    print("- Discovery: DHT with bootstrap server")
    print(f"- Log files: {paths.logs_dir}/node-*.log")
    print(f"- Bootstrap log: {paths.bootstrap_log}")
    unique_cfgs = sorted(set(config_cache.values()), key=lambda p: str(p))
    if unique_cfgs:
        print("- Committee configs:")
        for cfg in unique_cfgs:
            print(f"  - {cfg}")
    else:
        print("- Committee configs: none")
    print(f"- Server config: {paths.configs_dir / 'dev-server.json'}")
    print("")

    # Monitor nodes
    print(
        "Monitoring nodes (press Ctrl+C to stop all, or wait for automatic completion)..."
    )
    print(
        "Nodes will automatically stop when the committee-sampling simulation completes."
    )
    print("")

    completed = 0
    total = len(procs.node_procs)
    with tqdm(total=total, desc="Completed nodes") as pbar:
        while True:
            running_count = sum(1 for p in procs.node_procs if p.poll() is None)

            if total - running_count > completed:
                pbar.update(total - running_count - completed)
                completed = total - running_count
                pbar.set_postfix(completed=completed, running=running_count)

            if running_count == 0:
                print(
                    time.strftime("%H:%M:%S"),
                    "- All nodes have stopped. Waiting 30 seconds to confirm completion...",
                )
                print("")
                print("🎉 All committee-sampling nodes have completed successfully!")
                print(f"Simulation finished at: {datetime.now()}")
                print("")
                break
            time.sleep(30)

    print("Cleaning up and creating log archive...")
    cleanup(paths, procs, session_id)


def resolve_adversary_options(args: argparse.Namespace, run_config: Dict[str, object]) -> Tuple[float, AdversarySettings]:
    adversary_cfg = run_config.get("adversary", {}) if run_config else {}
    if not isinstance(adversary_cfg, dict):
        adversary_cfg = {}

    percent = float(run_config.get("adversary_percent", args.adversary_percent))
    enabled_flag = adversary_cfg.get("enabled", args.adversary_enabled)

    # If enabled explicitly but percent not provided, default to all nodes.
    if enabled_flag and percent <= 0:
        percent = 100.0
    # If percent provided but enabled flag omitted/false, treat as enabled.
    if percent > 0 and not enabled_flag:
        enabled_flag = True

    seed_value = adversary_cfg.get("seed", args.adversary_seed)
    seed = int(seed_value) if seed_value is not None else None

    drop_probability = float(
        adversary_cfg.get("drop_probability", args.adversary_drop_probability)
    )
    jitter_min = str(adversary_cfg.get("jitter_min", args.adversary_jitter_min or "0s"))
    jitter_max = str(adversary_cfg.get("jitter_max", args.adversary_jitter_max or "0s"))
    clock_skew = str(adversary_cfg.get("clock_skew", args.adversary_clock_skew or "0s"))

    ex_ante_cfg = adversary_cfg.get("ex_ante", {})
    if not isinstance(ex_ante_cfg, dict):
        ex_ante_cfg = {}
    ex_ante_equivocator = bool(
        ex_ante_cfg.get("equivocator", args.adversary_ex_ante_equivocator)
    )

    ex_post_cfg = adversary_cfg.get("ex_post", {})
    if not isinstance(ex_post_cfg, dict):
        ex_post_cfg = {}
    freshness_cfg = ex_post_cfg.get("freshness_cheater", adversary_cfg.get("freshness_cheater", {}))
    if not isinstance(freshness_cfg, dict):
        freshness_cfg = {}
    freshness_enabled = bool(
        freshness_cfg.get("enabled", args.adversary_ex_post_freshness_enabled)
    )
    freshness_mode = str(
        freshness_cfg.get("mode", args.adversary_ex_post_freshness_mode)
    ).lower()
    freshness_stale_rounds = int(
        freshness_cfg.get("stale_rounds", args.adversary_ex_post_freshness_stale_rounds)
    )
    freshness_truncate_leaf = bool(
        freshness_cfg.get("truncate_leaf", args.adversary_ex_post_freshness_truncate)
    )

    settings = AdversarySettings(
        seed=seed,
        drop_probability=drop_probability,
        jitter_min=jitter_min,
        jitter_max=jitter_max,
        clock_skew=clock_skew,
        ex_ante_equivocator=ex_ante_equivocator,
        freshness_enabled=freshness_enabled,
        freshness_mode=freshness_mode,
        freshness_stale_rounds=freshness_stale_rounds,
        freshness_truncate_leaf=freshness_truncate_leaf,
    )

    return percent if enabled_flag else 0.0, settings


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run committee-sampling simulations from a JSON config file"
    )
    parser.add_argument(
        "batch_config",
        help="Path to a JSON file describing multiple simulations to run sequentially",
    )
    parser.add_argument(
        "--default-log-level",
        dest="default_log_level",
        default="info",
        choices=["debug", "info", "warn", "error"],
        help="Default log level used when a run omits 'log_level' (default: info)",
    )
    parser.add_argument(
        "--drop-on-send-percent",
        dest="drop_on_send_percent",
        type=float,
        default=0.0,
        help="Percentage [0-100] of nodes per run that enable simulated drop-on-send (default: 0)",
    )
    parser.add_argument(
        "--drop-on-send-probability",
        dest="drop_on_send_probability",
        type=float,
        default=0.1,
        help="Probability [0-1] used when drop-on-send is enabled for a node (default: 0.1)",
    )
    parser.add_argument(
        "--adversary-percent",
        dest="adversary_percent",
        type=float,
        default=0.0,
        help="Percentage [0-100] of nodes per run that enable adversary behaviors (default: 0)",
    )
    parser.add_argument(
        "--adversary-enabled",
        dest="adversary_enabled",
        action="store_true",
        help="Enable adversary behaviors for all nodes when not overridden by the batch config",
    )
    parser.add_argument(
        "--adversary-seed",
        dest="adversary_seed",
        type=int,
        default=None,
        help="Seed for deterministic adversary simulations (default: None)",
    )
    parser.add_argument(
        "--adversary-drop-probability",
        dest="adversary_drop_probability",
        type=float,
        default=0.0,
        help="Drop probability [0-1] for adversarial network unreliability behavior (default: 0)",
    )
    parser.add_argument(
        "--adversary-jitter-min",
        dest="adversary_jitter_min",
        default="0s",
        help="Minimum adversarial jitter duration (default: 0s)",
    )
    parser.add_argument(
        "--adversary-jitter-max",
        dest="adversary_jitter_max",
        default="0s",
        help="Maximum adversarial jitter duration (default: 0s)",
    )
    parser.add_argument(
        "--adversary-clock-skew",
        dest="adversary_clock_skew",
        default="0s",
        help="Clock skew applied by adversary behaviors (default: 0s)",
    )
    parser.add_argument(
        "--adversary-ex-ante-equivocator",
        dest="adversary_ex_ante_equivocator",
        action="store_true",
        help="Enable the Ex-Ante equivocator adversary behavior",
    )
    parser.add_argument(
        "--adversary-ex-post-freshness-enabled",
        dest="adversary_ex_post_freshness_enabled",
        action="store_true",
        help="Enable the Ex-Post freshness cheater behavior",
    )
    parser.add_argument(
        "--adversary-ex-post-freshness-mode",
        dest="adversary_ex_post_freshness_mode",
        default="stale",
        choices=["stale", "truncate", "both"],
        help="Mode for the Ex-Post freshness cheater (default: stale)",
    )
    parser.add_argument(
        "--adversary-ex-post-freshness-stale-rounds",
        dest="adversary_ex_post_freshness_stale_rounds",
        type=int,
        default=1,
        help="Number of rounds to look back when replaying stale Ex-Post values (default: 1)",
    )
    parser.add_argument(
        "--adversary-ex-post-freshness-truncate",
        dest="adversary_ex_post_freshness_truncate",
        action="store_true",
        help="Truncate the last Merkle leaf in the freshness cheater behavior",
    )
    parser.add_argument(
        "--degree-slack",
        dest="degree_slack",
        type=int,
        default=0,
        help="Additional inbound degree slack beyond max_outbound_degree (default: 0)",
    )
    parser.add_argument(
        "--graph-discovery-timeout",
        dest="graph_discovery_timeout",
        default="30s",
        help="Duration to continue graph discovery before starting building rounds (default: 30s)",
    )
    parser.add_argument(
        "--graph-building-round-timeout",
        dest="graph_building_round_timeout",
        default="2m",
        help="Duration allocated per graph building round (default: 2m)",
    )
    parser.add_argument(
        "--graph-building-rounds",
        dest="graph_building_rounds",
        type=int,
        default=4,
        help="Number of graph building rounds to execute (must be even, default: 4)",
    )
    parser.add_argument(
        "--kill-random-up-to",
        dest="kill_random_up_to",
        type=int,
        default=0,
        help="If >0, randomly terminate up to this many node processes per run (default: 0 = disabled)",
    )
    parser.add_argument(
        "--kill-random-delay-sec",
        dest="kill_random_delay_sec",
        type=int,
        default=0,
        help="Seconds to wait before performing random kills (default: 0)",
    )
    parser.add_argument(
        "--kill-probability",
        dest="kill_probability",
        type=float,
        default=0.1,
        help="Probability [0-1] used when killing a node (default: 0.1)",
    )

    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    base_paths = Paths(
        base_dir=base_dir,
        bin_dir=base_dir / "bin",
        logs_dir=base_dir / "logs",  # base logs dir; per-run subdir created later
        configs_dir=base_dir / "configs",
        pids_file=base_dir
                  / "node_pids.txt",  # unused in batch; per-run file created later
        bootstrap_log=base_dir
                      / "bootstrap.log",  # unused in batch; per-run file created later
        bootstrap_pid_file=base_dir
                           / "bootstrap_pid.txt",  # unused in batch; per-run file created later
        bootstrap_address_file=base_dir / "bootstrap_address.txt",
    )

    cfg_path = Path(args.batch_config)
    if not cfg_path.is_file():
        sys.exit(f"Error: batch config file not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        try:
            batch_cfg = json.load(f)
        except json.JSONDecodeError as e:
            sys.exit(f"Error: failed to parse JSON: {e}")

    runs = batch_cfg.get("runs")
    if not isinstance(runs, list) or not runs:
        sys.exit("Error: batch config must contain a non-empty 'runs' array")
    sleep_between = int(batch_cfg.get("sleep_between_runs_sec", 0) or 0)

    for idx, run in enumerate(runs, start=1):
        # Flexible field names
        num_nodes = run.get("number_of_nodes", run.get("num_nodes"))
        max_deg = run.get("max_outbound_degree", run.get("max_degree"))
        diameter = run.get("diameter")
        log_level = run.get("log_level", args.default_log_level)
        drop_on_send_percent = float(
            run.get("drop_on_send_percent", args.drop_on_send_percent)
        )
        drop_on_send_probability = float(
            run.get("drop_on_send_probability", args.drop_on_send_probability)
        )
        adversary_percent, adversary_settings = resolve_adversary_options(args, run)
        degree_slack = int(run.get("degree_slack", args.degree_slack))
        kill_random_up_to = int(run.get("kill_random_up_to", args.kill_random_up_to))
        kill_random_delay_sec = int(
            run.get("kill_random_delay_sec", args.kill_random_delay_sec)
        )
        kill_probability = float(run.get("kill_probability", args.kill_probability))
        verify_timeout = str(run.get("verify_timeout", "30s"))
        committee_size = int(run.get("committee_size", 30))
        graph_discovery_timeout = run.get("graph_discovery_timeout")
        if not graph_discovery_timeout:
            graph_discovery_timeout = args.graph_discovery_timeout
        graph_discovery_timeout = str(graph_discovery_timeout)

        graph_building_round_timeout = run.get("graph_building_round_timeout")
        if not graph_building_round_timeout:
            graph_building_round_timeout = run.get("building_graph_timeout")
        if not graph_building_round_timeout:
            graph_building_round_timeout = args.graph_building_round_timeout
        graph_building_round_timeout = str(graph_building_round_timeout)

        graph_building_rounds = run.get("graph_building_rounds")
        if graph_building_rounds is None:
            graph_building_rounds = run.get("building_rounds")
        if graph_building_rounds is None:
            graph_building_rounds = args.graph_building_rounds
        graph_building_rounds = int(graph_building_rounds)

        if num_nodes is None or max_deg is None or diameter is None:
            sys.exit(
                f"Error: run #{idx} must include 'number_of_nodes' (or 'num_nodes'), 'max_outbound_degree' (or 'max_degree'), and 'diameter'"
            )

        run_label = f"run-{idx:02d}-{num_nodes}n-{max_deg}m-d{diameter}"
        run_one_simulation(
            base_paths,
            num_nodes=int(num_nodes),
            max_outbound_degree=int(max_deg),
            diameter=int(diameter),
            log_level=str(log_level),
            run_label=run_label,
            drop_on_send_percent=drop_on_send_percent,
            drop_on_send_probability=drop_on_send_probability,
            adversary_percent=adversary_percent,
            adversary_settings=adversary_settings,
            degree_slack=degree_slack,
            kill_random_up_to=kill_random_up_to,
            kill_random_delay_sec=kill_random_delay_sec,
            kill_probability=kill_probability,
            verify_timeout=verify_timeout,
            committee_size=committee_size,
            graph_building_rounds=graph_building_rounds,
            graph_discovery_timeout=graph_discovery_timeout,
            graph_building_round_timeout=graph_building_round_timeout,
        )

        if idx < len(runs) and sleep_between > 0:
            print(f"Sleeping {sleep_between}s before the next run...")
            time.sleep(sleep_between)


if __name__ == "__main__":
    main()
