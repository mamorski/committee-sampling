#!/bin/bash

# Script to run committee-sampling simulation with configurable number of nodes
# Usage: ./run_simulation.sh <number_of_nodes>

set -e

# Check if number of nodes is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <number_of_nodes>"
    echo "Example: $0 20"
    exit 1
fi

NUM_NODES=$1

# Validate number of nodes
if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [ "$NUM_NODES" -lt 2 ]; then
    echo "Error: Number of nodes must be a positive integer >= 2"
    exit 1
fi

# Configuration
SESSION_ID="simulation-$(date +%s)"
LOGS_DIR="./logs"
CONFIGS_DIR="./configs"
PIDS_FILE="./node_pids.txt"
BIN_DIR="./bin"

echo "🚀 Starting $NUM_NODES committee-sampling simulation nodes..."
echo "Session ID: $SESSION_ID"
echo "Logs directory: $LOGS_DIR"
echo "Configs directory: $CONFIGS_DIR"
echo ""

# Check if binaries exist
if [ ! -f "$BIN_DIR/server" ]; then
    echo "Error: Server binary not found at $BIN_DIR/server"
    exit 1
fi

if [ ! -f "$BIN_DIR/committee-sampling" ]; then
    echo "Error: Committee-sampling binary not found at $BIN_DIR/committee-sampling"
    exit 1
fi

if [ ! -f "$CONFIGS_DIR/dev-server.json" ]; then
    echo "Error: Server config not found at $CONFIGS_DIR/dev-server.json"
    exit 1
fi

# Create directories
mkdir -p "$LOGS_DIR"
mkdir -p "$CONFIGS_DIR"

# Clear previous PIDs file
> "$PIDS_FILE"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Stopping all nodes..."
    if [ -f "$PIDS_FILE" ]; then
        while IFS= read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "Stopping node with PID: $pid"
                kill "$pid"
            fi
        done < "$PIDS_FILE"
    fi
    
    # Stop bootstrap server
    if [ -f "bootstrap_pid.txt" ]; then
        BOOTSTRAP_PID=$(cat bootstrap_pid.txt)
        if kill -0 "$BOOTSTRAP_PID" 2>/dev/null; then
            echo "Stopping bootstrap server (PID: $BOOTSTRAP_PID)"
            kill "$BOOTSTRAP_PID"
        fi
        rm -f bootstrap_pid.txt
    fi
    
    # Create backup of logs for debugging
    if [ -d "$LOGS_DIR" ]; then
        BACKUP_DIR="./logs-backup-$(date +%Y%m%d-%H%M%S)"
        echo "Creating backup of logs at: $BACKUP_DIR"
        cp -r "$LOGS_DIR" "$BACKUP_DIR"
        
        # Show a summary of what happened
        echo ""
        echo "Debugging Summary:"
        echo "- Bootstrap server log: bootstrap.log"
        echo "- Node logs backup: $BACKUP_DIR/"
        echo "- Neighbor connection summary:"
        
        # Count successful neighbor connections
        if [ -f "bootstrap.log" ]; then
            echo "  - Bootstrap connections: $(grep -c "Node connected to DHT" bootstrap.log || echo "0")"
        fi
        
        # Show peer discovery attempts
        if ls "$BACKUP_DIR"/node-*-log.log 1> /dev/null 2>&1; then
            echo "  - Peer discovery attempts: $(grep -c "Attempting to connect to discovered peer" "$BACKUP_DIR"/node-*-log.log 2>/dev/null || echo "0")"
            echo "  - Successful neighbor additions: $(grep -c "Successfully added neighbor" "$BACKUP_DIR"/node-*-log.log 2>/dev/null || echo "0")"
        fi
        
        echo ""
        echo "To debug further:"
        echo "  - Check bootstrap server: cat bootstrap.log"
        echo "  - Check node logs: ls $BACKUP_DIR/"
        echo "  - Monitor peer discovery: grep 'Attempting to connect' $BACKUP_DIR/node-*-log.log"
        echo "  - Check neighbor connections: grep 'Successfully added neighbor' $BACKUP_DIR/node-*-log.log"
    fi
    
    # Clean up temporary logs (but keep configs as requested)
    echo "Cleaning up temporary logs..."
    rm -rf "$LOGS_DIR"
    rm -f "$PIDS_FILE"
    rm -f "bootstrap_address.txt"
    
    echo "All nodes and bootstrap server stopped"
    echo "Logs backed up for debugging - check the logs-backup-* directory"
    echo "Configuration files preserved in $CONFIGS_DIR/"
    exit 0
}

# Trap signals to cleanup
trap cleanup SIGINT SIGTERM

# Function to calculate max_outbound_degree = log2(number of nodes)
calculate_max_outbound_degree() {
    local n=$1
    # Calculate log2(n) using natural logarithm: log2(n) = ln(n) / ln(2)
    # Use bc for floating point arithmetic and round to nearest integer
    local result=$(echo "scale=10; l($n)/l(2)" | bc -l)
    # Round to nearest integer
    local rounded=$(echo "scale=0; ($result + 0.5)/1" | bc)
    # Ensure minimum value of 1
    if [ "$rounded" -lt 1 ]; then
        echo 1
    else
        echo "$rounded"
    fi
}

# Function to calculate diameter = ln(n) / ln(ln(n)-1)
calculate_diameter() {
    local n=$1
    
    # For small values, use default diameter
    if [ "$n" -le 3 ]; then
        echo 4
        return
    fi
    
    # Calculate ln(n) and ln(ln(n)-1)
    local ln_n=$(echo "scale=10; l($n)" | bc -l)
    local ln_n_minus_1=$(echo "scale=10; $ln_n - 1" | bc -l)
    
    # Check if ln(n)-1 > 0 to avoid ln of negative/zero
    local gt_zero=$(echo "$ln_n_minus_1 > 0" | bc -l)
    if [ "$gt_zero" -eq 0 ]; then
        echo 4
        return
    fi
    
    local ln_ln_n_minus_1=$(echo "scale=10; l($ln_n_minus_1)" | bc -l)
    local diameter=$(echo "scale=10; $ln_n / $ln_ln_n_minus_1" | bc -l)
    
    # Round to nearest integer and ensure minimum value of 2
    local rounded=$(echo "scale=0; ($diameter + 0.5)/1" | bc)
    if [ "$rounded" -lt 2 ]; then
        echo 2
    else
        echo "$rounded"
    fi
}

# Calculate network parameters
MAX_OUTBOUND_DEGREE=$(calculate_max_outbound_degree $NUM_NODES)
DIAMETER=$(calculate_diameter $NUM_NODES)

echo "Calculated network parameters:"
echo "- Max outbound degree: $MAX_OUTBOUND_DEGREE (log2($NUM_NODES))"
echo "- Diameter: $DIAMETER (ln($NUM_NODES)/ln(ln($NUM_NODES)-1))"
echo ""

# Function to create committee-sampling config file
create_committee_config() {
    local config_file="$CONFIGS_DIR/committee-sampling-conf.json"
    
    cat > "$config_file" << EOF
{
  "network": {
    "listen_port": 0,
    "max_outbound_degree": $MAX_OUTBOUND_DEGREE,
    "heartbeat_interval": "30s",
    "connect_timeout": "20s",
    "topic": "committee-sampling",
    "find_peers_timeout": "2m",
    "discovery_config": {
      "discovery_type": "dht",
      "protocol_id": "/committee-sampling/1.0.0",
      "interval": "5s",
      "bootstrap_peers": ["$BOOTSTRAP_ADDRESS"],
      "service_tag": ""
    }
  },
  "graph": {
    "diameter": $DIAMETER,
    "grading_levels": 5
  },
  "run_time": {
    "session_id": "$SESSION_ID",
    "lambda": 384,
    "weight": 10,
    "delta_w": 3.0,
    "committee_size": 4,
    "delay": 20
  },
  "synchronization": {
    "type": 1,
    "ex_ante_round_timeout": "10s",
    "ex_post_round_timeout": "10s",
    "mdag_round_timeout": "10s",
    "start_time": $(($(date +%s) + 60)),
    "building_graph_timeout": "1m",
    "time_server": "time.google.com",
    "certificate_path": "/home/igor/repos/committee-sampling-server/certs/sync-sender.crt",
    "topic": "sync-topic"
  },
  "logger": {
    "level": "debug"
  }
}
EOF
    
    echo "$config_file"
}

# Function to start a node
start_node() {
    local node_id=$1
    local config_file=$2
    local log_file="$LOGS_DIR/node-$node_id-log.log"
    
    echo "Starting node-$node_id with config: $config_file..."
    
    # Start the node in the background with -config flag
    "$BIN_DIR/committee-sampling" -config "$config_file" > "$log_file" 2>&1 &
    
    # Save PID
    local pid=$!
    echo "$pid" >> "$PIDS_FILE"
    
    echo "Node-$node_id started (PID: $pid, Port: auto-assigned, Log: $log_file)"
}

# Start bootstrap server first
echo "Starting DHT bootstrap server..."

# Start bootstrap server using the compiled binary and existing config
"$BIN_DIR/server" -config "$CONFIGS_DIR/dev-server.json" > bootstrap.log 2>&1 &
BOOTSTRAP_PID=$!
echo "$BOOTSTRAP_PID" > bootstrap_pid.txt

echo "Bootstrap server started (PID: $BOOTSTRAP_PID)"
echo "Waiting 10 seconds for bootstrap server to be ready..."
sleep 10

# Read the bootstrap address from the file
BOOTSTRAP_ADDRESS_FILE="./bootstrap_address.txt"
if [ -f "$BOOTSTRAP_ADDRESS_FILE" ]; then
    BOOTSTRAP_ADDRESS=$(cat "$BOOTSTRAP_ADDRESS_FILE")
    echo "Bootstrap address: $BOOTSTRAP_ADDRESS"
else
    echo "Failed to read bootstrap address from file"
    echo "Check bootstrap.log for the bootstrap address and update the script manually"
    exit 1
fi

# Create config file
echo ""
echo "Creating committee-sampling configuration..."
config_file=$(create_committee_config)

# Start all nodes
echo ""
echo "Starting nodes..."
for i in $(seq 1 $NUM_NODES); do
    start_node "$i" "$config_file"
done

echo ""
echo "All $NUM_NODES nodes started successfully!"
echo ""
echo "Simulation Status:"
echo "- Nodes: $NUM_NODES"
echo "- Max outbound degree: $MAX_OUTBOUND_DEGREE"
echo "- Diameter: $DIAMETER"
echo "- Ports: Auto-assigned by system (port 0 configured)"
echo "- Session ID: $SESSION_ID"
echo "- Discovery: DHT with bootstrap server"
echo "- Log files: $LOGS_DIR/node-*-log.log"
echo "- Bootstrap log: bootstrap.log"
echo "- Committee config: $config_file"
echo "- Server config: $CONFIGS_DIR/dev-server.json"
echo ""
echo "Useful commands:"
echo "- Monitor node logs: tail -f $LOGS_DIR/node-1-log.log"
echo "- Monitor all node logs: tail -f $LOGS_DIR/*.log"
echo "- Monitor bootstrap server: tail -f bootstrap.log"
echo "- Check running processes: ps aux | grep committee-sampling"
echo "- Stop all nodes: Press Ctrl+C"
echo ""

# Monitor nodes
echo "Monitoring nodes (press Ctrl+C to stop all)..."
while true; do
    running_count=0
    if [ -f "$PIDS_FILE" ]; then
        while IFS= read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                running_count=$((running_count + 1))
            fi
        done < "$PIDS_FILE"
    fi
    
    echo "$(date '+%H:%M:%S') - Running nodes: $running_count/$NUM_NODES"
    sleep 10
done