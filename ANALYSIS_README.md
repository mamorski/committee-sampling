# Simulation Analysis Notebook

## Overview

The `simulation_analysis.ipynb` notebook provides comprehensive analysis of committee sampling simulation logs. It extracts and visualizes:

1. **Graph Properties**: Network structure, degree distributions, connectivity
2. **Protocol Messaging**: Message statistics for MDAG, ExAnte, and ExPost protocols
3. **Committee Analysis**: Election results, consensus checking, and correlation metrics

## Requirements

Install the required Python packages:

```bash
pip install numpy pandas networkx matplotlib seaborn scipy jupyter
```

Or use the existing virtual environment:

```bash
source venv/bin/activate
```

## Usage

### 1. Run a Simulation

First, generate simulation logs using the simulation script:

```bash
python scripts/run_simulations.py configs/sample_simulation_plan.json
```

This will create log archives (e.g., `simulation-*.tar.gz`).

### 2. Extract Logs

Extract the simulation logs to a directory:

```bash
mkdir -p analysis_logs
tar -xzf simulation-20251018-181638-n1000-m12-d6-c20.tar.gz -C analysis_logs
```

### 3. Run the Analysis Notebook

Launch Jupyter:

```bash
jupyter notebook simulation_analysis.ipynb
```

Update the `LOG_DIRECTORY` variable in cell 4 to point to your extracted logs:

```python
LOG_DIRECTORY = "./analysis_logs/simulation-20251018-181638-n1000-m12-d6-c20"
```

Then run all cells to generate the complete analysis.

## Notebook Sections

### 1. Setup and Imports
Loads required libraries and sets up visualization styles.

### 2. Configuration
Set the path to your log directory.

### 3. Utility Functions
Helper functions for parsing JSON log files and extracting node IDs.

### 4. Load Log Data
Reads all `node-*.log` files from the directory.

### 5. Graph Analysis
- Extracts the directed graph from neighbor lists
- Computes metrics: degree distributions, one-directional edges, connectivity
- Checks if graph is strongly connected
- Calculates diameter and connected components
- Visualizes degree distributions

### 6. Protocol Messaging Statistics
- Extracts MDAG message counts per round
- Tracks valid vs total messages
- Calculates acceptance rates
- Shows per-round statistics: mean, std, min, max
- Visualizes message patterns over rounds

### 7. Committee Analysis
- Parses committee election results from logs
- Extracts member ID, verification key (VK), and grade
- Computes committee size statistics
- Visualizes size distributions

### 8. Committee Consensus Analysis
- Checks if all nodes elected identical committees
- Reports consensus status

### 9. Committee Correlation Analysis
- Computes pairwise Jaccard similarity between committees
- Generates similarity matrix
- Visualizes as heatmap (for ≤50 nodes) or histogram

### 10. Grade Correlation Analysis
- Identifies shared committee members across nodes
- Computes Pearson correlation of grades
- Visualizes correlation distribution

### 11. Summary Dashboard
Consolidated view of all key metrics from the analysis.

## Output Metrics

### Graph Metrics
- Number of nodes and edges
- Average in-degree and out-degree
- Standard deviation of degrees
- One-directional vs bidirectional edges
- Strong connectivity status
- Number of strongly connected components
- Graph diameter (if strongly connected)

### Messaging Metrics
Per round statistics:
- Valid message count (mean, std, min, max, sum)
- Total message count (mean, std, min, max, sum)
- Acceptance rate (valid/total)

### Committee Metrics
- Committee size range (min, max, mean, median, std)
- Consensus status (same committee across all nodes)
- Jaccard similarity scores (mean, std, min, max, median)
- Grade correlation (Pearson coefficient)

## Interpreting Results

### Graph Health
- **Strongly connected**: All nodes can reach each other
- **Low one-directional edges**: Better bidirectional communication
- **Average degree close to configured max**: Good graph construction

### Committee Consensus
- **Jaccard similarity = 1.0**: Perfect agreement
- **Jaccard similarity > 0.8**: High agreement
- **Jaccard similarity < 0.5**: Low agreement, potential issues

### Grade Correlation
- **High positive correlation**: Consistent grading across nodes
- **Low or negative correlation**: Inconsistent grading, investigate causes

## Troubleshooting

### No Data Found
- Ensure log files are in JSON format
- Check that `node-*.log` files exist in the directory
- Verify log files contain expected JSON entries

### Empty Statistics
- Some protocols may not log certain messages at all log levels
- Check that simulation ran long enough to generate messages
- Verify log level is set to DEBUG or INFO for detailed messages

### Memory Issues
- For very large simulations (>10,000 nodes), consider:
  - Processing logs in batches
  - Sampling a subset of nodes
  - Using memory-efficient data structures

## Extending the Notebook

To add custom analysis:

1. Create a new cell after the relevant section
2. Access parsed data from variables:
   - `all_logs`: Dict of all log entries by node
   - `graph`: NetworkX DiGraph
   - `mdag_stats`: DataFrame of MDAG statistics
   - `committees`: Dict of committee data by node

Example:
```python
# Custom analysis: per-node messaging statistics
for node_id, df in mdag_stats['ExAnte'].groupby('node_id'):
    avg_valid = df['valid_messages'].mean()
    avg_total = df['total_messages'].mean()
    acceptance = avg_valid / avg_total if avg_total > 0 else 0
    print(f"Node {node_id}: {avg_valid:.2f} valid, {acceptance:.2%} acceptance rate")
```

## Contact

For issues or questions about the analysis notebook, please refer to the main project documentation.


