#!/usr/bin/env python3
"""Run committee sampling simulations sequentially based on a JSON plan.

The JSON configuration must be an array; each element describes one simulation
run. Example entry (non-exhaustive):

[
  {
    "name": "baseline",
    "base_config": "configs/dev.json",
    "duration_seconds": 120,
    "bootstrap_wait_seconds": 5,
    "global_overrides": {
      "network": {
        "max_outbound_degree": 4,
        "drop_on_send": false
      },
      "graph": {
        "diameter": 4,
        "grading_levels": 3
      }
    },
    "nodes": [
      {
        "id": "honest-1",
        "overrides": {
          "network": {
            "adversary": {"enabled": false}
          }
        }
      },
      {
        "id": "adversarial-1",
        "overrides": {
          "network": {
            "adversary": {
              "enabled": true,
              "drop_probability": 0.25
            }
          }
        }
      }
    ]
  }
]

Keys:
- name: Optional identifier for the run. Used in directory names.
- base_config: Optional path to a template JSON config (default: configs/dev.json).
- duration_seconds: Optional duration before the runner stops the processes.
- bootstrap_wait_seconds: Optional extra delay after bootstrap server starts.
- stop_timeout_seconds: Optional graceful shutdown window before processes are killed.
- global_overrides: Optional dict merged into every node config.
- nodes: Either an integer (number of identical nodes) or a list of node
  descriptors. Each descriptor may contain:
  * id: Optional human-friendly id (defaults to node-1, node-2, ...).
  * overrides: Dict merged on top of the base+global config for this node.
  * env: Optional environment variables dict for this node process.

The script expects the following files/binaries to exist relative to the
repository root (adjust via CLI flags if needed):
- ./bin/server
- ./bin/committee-sampling
- bootstrap_address.txt
- dev-server.json (or custom via --server-config)

Outputs for each simulation run include generated configs, process logs, and a
compressed tarball containing the run directory.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import signal
import subprocess
import sys
import tarfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Tuple

DEFAULT_BASE_CONFIG = Path("configs/dev.json")


class SimulationError(Exception):
    """Raised when a simulation run cannot be completed."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run committee sampling simulations sequentially.")
    parser.add_argument("config", type=Path, help="Path to the JSON array describing the simulations.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("simulation_runs"),
        help="Directory where run artifacts (logs, configs, archives) are stored.",
    )
    parser.add_argument(
        "--bin-dir",
        type=Path,
        default=Path("bin"),
        help="Directory containing the compiled binaries (committee-sampling, server).",
    )
    parser.add_argument(
        "--server-config",
        type=Path,
        default=Path("dev-server.json"),
        help="Configuration file for the bootstrap server.",
    )
    parser.add_argument(
        "--bootstrap-address-file",
        type=Path,
        default=Path("bootstrap_address.txt"),
        help="File containing the bootstrap peer multiaddress written by the server.",
    )
    parser.add_argument(
        "--default-duration",
        type=int,
        default=None,
        help="Optional default duration (seconds) used when a run omits duration_seconds.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def ensure_executable(path: Path, description: str) -> None:
    if not path.exists():
        raise SimulationError(f"{description} not found at {path}")
    if not os.access(path, os.X_OK):
        raise SimulationError(f"{description} at {path} is not executable")


def deep_merge(base: MutableMapping[str, Any], overrides: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Recursively merge `overrides` into `base` (mutating and returning base)."""
    for key, value in overrides.items():
        if (
            key in base
            and isinstance(base[key], MutableMapping)
            and isinstance(value, MutableMapping)
        ):
            deep_merge(base[key], value)
        else:
            base[key] = copy.deepcopy(value)
    return base


def prepare_node_configs(
    run_spec: Dict[str, Any],
    base_config_data: Dict[str, Any],
    bootstrap_address: str,
) -> List[Tuple[str, Dict[str, Any], Dict[str, str]]]:
    nodes_spec = run_spec.get("nodes")
    if nodes_spec is None:
        raise SimulationError("Run spec must define 'nodes' (integer or array).")

    if isinstance(nodes_spec, int):
        if nodes_spec <= 0:
            raise SimulationError("'nodes' must be > 0 when provided as an integer.")
        nodes: List[Dict[str, Any]] = [
            {"id": f"node-{i+1}", "overrides": {}} for i in range(nodes_spec)
        ]
    elif isinstance(nodes_spec, list):
        nodes = []
        for idx, entry in enumerate(nodes_spec):
            if not isinstance(entry, MutableMapping):
                raise SimulationError(
                    "Each node entry must be an object with optional 'id', 'overrides', and 'env'."
                )
            node_id = str(entry.get("id", f"node-{idx + 1}"))
            overrides = entry.get("overrides", {})
            if overrides and not isinstance(overrides, MutableMapping):
                raise SimulationError(f"Node {node_id} overrides must be an object.")
            env = entry.get("env", {})
            if env and not isinstance(env, MutableMapping):
                raise SimulationError(f"Node {node_id} env must be an object of key/value pairs.")
            nodes.append({"id": node_id, "overrides": overrides or {}, "env": {str(k): str(v) for k, v in env.items()}})
    else:
        raise SimulationError("'nodes' must be either an integer or an array of objects.")

    global_overrides = run_spec.get("global_overrides", {})
    if global_overrides and not isinstance(global_overrides, MutableMapping):
        raise SimulationError("'global_overrides' must be an object when provided.")

    configs: List[Tuple[str, Dict[str, Any], Dict[str, str]]] = []

    for node_entry in nodes:
        node_id = node_entry["id"]
        node_config = copy.deepcopy(base_config_data)
        if global_overrides:
            deep_merge(node_config, global_overrides)
        if node_entry["overrides"]:
            deep_merge(node_config, node_entry["overrides"])

        # Ensure bootstrap peer is set.
        network = node_config.setdefault("network", {})
        discovery = network.setdefault("discovery_config", {})
        discovery["bootstrap_peers"] = [bootstrap_address]
        configs.append((node_id, node_config, node_entry.get("env", {})))

    return configs


def start_process(command: List[str], log_path: Path, env: Optional[Dict[str, str]] = None) -> Tuple[subprocess.Popen[Any], Any]:
    log_file = log_path.open("wb")
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    try:
        process = subprocess.Popen(
            command,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=merged_env,
        )
    except Exception:
        log_file.close()
        raise
    return process, log_file


def stop_process(process: subprocess.Popen[Any], name: str, timeout: int) -> None:
    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.SIGINT)
    except Exception:
        process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def wait_for_bootstrap_address(path: Path, timeout: int = 15) -> str:
    deadline = time.time() + timeout
    last_content: str = ""
    while time.time() < deadline:
        if path.exists():
            content = path.read_text(encoding="utf-8").strip()
            if content:
                last_content = content
                if "\n" in content:
                    # Use the first non-empty line.
                    for line in content.splitlines():
                        line = line.strip()
                        if line:
                            return line
                return content
        time.sleep(0.5)
    if last_content:
        return last_content
    raise SimulationError(f"Bootstrap address file {path} not populated within timeout")


def archive_run(run_dir: Path, archive_path: Path) -> None:
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(run_dir, arcname=run_dir.name)


def run_simulation(
    idx: int,
    run_spec: Dict[str, Any],
    binaries: Dict[str, Path],
    server_config: Path,
    bootstrap_file: Path,
    output_root: Path,
    default_duration: Optional[int],
) -> None:
    run_name = str(run_spec.get("name", f"run-{idx+1}"))
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    run_dir = output_root / f"{timestamp}_{run_name}"
    configs_dir = run_dir / "configs"
    logs_dir = run_dir / "logs"
    run_dir.mkdir(parents=True, exist_ok=False)
    configs_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    base_config_path = Path(run_spec.get("base_config", DEFAULT_BASE_CONFIG))
    if not base_config_path.exists():
        raise SimulationError(f"Base config {base_config_path} does not exist")

    base_config_data = load_json(base_config_path)

    duration = run_spec.get("duration_seconds", default_duration)
    if duration is not None:
        if not isinstance(duration, int) or duration <= 0:
            raise SimulationError("duration_seconds/default-duration must be a positive integer when provided")

    stop_timeout = run_spec.get("stop_timeout_seconds", 15)
    if not isinstance(stop_timeout, int) or stop_timeout <= 0:
        raise SimulationError("stop_timeout_seconds must be a positive integer when provided")

    bootstrap_wait = run_spec.get("bootstrap_wait_seconds", 3)
    if not isinstance(bootstrap_wait, (int, float)) or bootstrap_wait < 0:
        raise SimulationError("bootstrap_wait_seconds must be a non-negative number when provided")

    server_log = logs_dir / "bootstrap.log"
    node_processes: List[Tuple[str, subprocess.Popen[Any], Any]] = []
    open_files: List[Any] = []

    server_cmd = [str(binaries["server"]), "-config", str(server_config)]
    print(f"[run {run_name}] Starting bootstrap server: {' '.join(server_cmd)}")
    server_proc, server_log_file = start_process(server_cmd, server_log)
    open_files.append(server_log_file)

    try:
        time.sleep(max(bootstrap_wait, 0))
        bootstrap_address = wait_for_bootstrap_address(bootstrap_file)

        node_configs = prepare_node_configs(run_spec, base_config_data, bootstrap_address)

        node_logs: Dict[str, Path] = {}
        node_config_paths: Dict[str, Path] = {}

        for node_id, config_data, env in node_configs:
            node_config_path = configs_dir / f"{node_id}.json"
            with node_config_path.open("w", encoding="utf-8") as fp:
                json.dump(config_data, fp, indent=2)
            node_config_paths[node_id] = node_config_path

            log_path = logs_dir / f"{node_id}.log"
            node_logs[node_id] = log_path

            node_cmd = [str(binaries["node"]), "-config", str(node_config_path)]
            print(f"[run {run_name}] Starting node {node_id}: {' '.join(node_cmd)}")
            proc, log_file = start_process(node_cmd, log_path, env=env)
            node_processes.append((node_id, proc, log_file))
            open_files.append(log_file)

        start_time = time.time()
        if duration is not None:
            print(f"[run {run_name}] Running for {duration} seconds...")
            end_time = start_time + duration
            while time.time() < end_time:
                if any(proc.poll() is not None for _, proc, _ in node_processes):
                    break
                time.sleep(1)
        else:
            print(f"[run {run_name}] No duration specified, waiting for nodes to exit...")
            while any(proc.poll() is None for _, proc, _ in node_processes):
                time.sleep(1)

        # Initiate shutdown.
        print(f"[run {run_name}] Initiating shutdown...")
        for node_id, proc, _ in node_processes:
            stop_process(proc, f"node {node_id}", timeout=stop_timeout)

        stop_process(server_proc, "bootstrap server", timeout=stop_timeout)

        # Report exit codes.
        for node_id, proc, _ in node_processes:
            print(f"[run {run_name}] Node {node_id} exit code: {proc.returncode}")
        print(f"[run {run_name}] Bootstrap server exit code: {server_proc.returncode}")

    finally:
        for _, proc, _ in node_processes:
            if proc.poll() is None:
                proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        if server_proc.poll() is None:
            server_proc.kill()
        try:
            server_proc.wait(timeout=5)
        except Exception:
            pass
        for fh in open_files:
            try:
                fh.close()
            except Exception:
                pass

    archive_path = run_dir.with_suffix(".tar.gz")
    archive_run(run_dir, archive_path)
    print(f"[run {run_name}] Archived logs and configs to {archive_path}")


def main() -> int:
    args = parse_args()

    try:
        run_specs = load_json(args.config)
    except FileNotFoundError as exc:
        print(f"Config file not found: {exc}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as exc:
        print(f"Failed to parse config JSON: {exc}", file=sys.stderr)
        return 1

    if not isinstance(run_specs, list):
        print("Top-level config must be a JSON array.", file=sys.stderr)
        return 1

    binaries = {
        "node": args.bin_dir / "committee-sampling",
        "server": args.bin_dir / "server",
    }

    try:
        ensure_executable(binaries["node"], "committee-sampling binary")
        ensure_executable(binaries["server"], "bootstrap server binary")
    except SimulationError as exc:
        print(exc, file=sys.stderr)
        return 1

    if not args.server_config.exists():
        print(f"Server config {args.server_config} does not exist.", file=sys.stderr)
        return 1

    output_root = args.output_dir
    output_root.mkdir(parents=True, exist_ok=True)

    for idx, run_spec in enumerate(run_specs):
        if not isinstance(run_spec, MutableMapping):
            print(f"Run spec at index {idx} must be an object.", file=sys.stderr)
            return 1
        try:
            run_simulation(
                idx=idx,
                run_spec=run_spec,
                binaries=binaries,
                server_config=args.server_config,
                bootstrap_file=args.bootstrap_address_file,
                output_root=output_root,
                default_duration=args.default_duration,
            )
        except SimulationError as exc:
            print(f"Run {idx + 1} failed: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:  # noqa: BLE001
            print(f"Run {idx + 1} encountered an unexpected error: {exc}", file=sys.stderr)
            return 1

    print("All simulations completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
