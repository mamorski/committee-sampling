#!/usr/bin/env python3

import json
import re
import os
from collections import defaultdict, deque
import matplotlib.pyplot as plt
import networkx as nx
from datetime import datetime

class NetworkAnalyzer:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.node_to_id = {}
        self.id_to_node = {}
        self.neighbors = defaultdict(set)
        self.node_connections = defaultdict(list)
        self.committee_results = {}
        
    def extract_node_id(self, log_filename):
        """Extract node number from log filename"""
        match = re.search(r'node-(\d+)-log\.log', log_filename)
        return int(match.group(1)) if match else None
        
    def parse_log_file(self, filepath):
        """Parse a single log file for neighbor and committee information"""
        node_num = self.extract_node_id(os.path.basename(filepath))
        if node_num is None:
            return
            
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                # Skip non-JSON lines
                if not line.startswith('{'):
                    continue
                    
                try:
                    log_entry = json.loads(line)
                    self.process_log_entry(log_entry, node_num)
                except json.JSONDecodeError:
                    continue
                    
    def process_log_entry(self, log_entry, node_num):
        """Process a single log entry"""
        msg = log_entry.get('msg', '')
        
        # Extract node ID mapping
        if 'node_id' in log_entry:
            node_id = log_entry['node_id']
            self.node_to_id[node_num] = node_id
            self.id_to_node[node_id] = node_num
            
        # Extract neighbor relationships
        if msg == "Successfully added neighbor":
            self.process_neighbor_addition(log_entry, node_num)
            
        # Extract committee information
        if msg == "Committee elected":
            self.process_committee_election(log_entry, node_num)
            
    def process_neighbor_addition(self, log_entry, node_num):
        """Process neighbor addition"""
        peer_id = log_entry.get('peer_id')
        if peer_id:
            self.node_connections[node_num].append({
                'peer_id': peer_id,
                'timestamp': log_entry.get('timestamp'),
                'total_neighbors': log_entry.get('total_neighbors', 0)
            })
            
    def process_committee_election(self, log_entry, node_num):
        """Process committee election results"""
        committee = log_entry.get('committee', [])
        self.committee_results[node_num] = {
            'committee': committee,
            'timestamp': log_entry.get('timestamp'),
            'node_id': log_entry.get('node_id')
        }
        
    def build_graph(self):
        """Build the neighbor graph"""
        # Create mapping from peer_id to node number
        peer_to_node = {}
        for node_num, node_id in self.node_to_id.items():
            peer_to_node[node_id] = node_num
            
        # Build neighbor relationships
        for node_num, connections in self.node_connections.items():
            for conn in connections:
                peer_id = conn['peer_id']
                if peer_id in peer_to_node:
                    peer_node = peer_to_node[peer_id]
                    self.neighbors[node_num].add(peer_node)
                    
    def find_one_directional_edges(self):
        """Find edges that exist in only one direction"""
        one_directional = []
        
        for node, neighbors in self.neighbors.items():
            for neighbor in neighbors:
                if node not in self.neighbors[neighbor]:
                    one_directional.append((node, neighbor))
                    
        return one_directional
        
    def calculate_diameter(self):
        """Calculate graph diameter using BFS"""
        if not self.neighbors:
            return 0
            
        max_distance = 0
        nodes = list(self.neighbors.keys())
        
        for start_node in nodes:
            distances = self.bfs_distances(start_node)
            if distances:
                max_dist = max(distances.values())
                max_distance = max(max_distance, max_dist)
                
        return max_distance
        
    def bfs_distances(self, start_node):
        """Calculate distances from start_node to all reachable nodes"""
        distances = {start_node: 0}
        queue = deque([start_node])
        
        while queue:
            current = queue.popleft()
            current_dist = distances[current]
            
            for neighbor in self.neighbors[current]:
                if neighbor not in distances:
                    distances[neighbor] = current_dist + 1
                    queue.append(neighbor)
                    
        return distances
        
    def analyze_committee_consensus(self):
        """Analyze committee consensus across nodes"""
        if not self.committee_results:
            return {"consensus": False, "reason": "No committee results found"}
            
        # Group by committee composition
        committee_groups = defaultdict(list)
        
        for node_num, result in self.committee_results.items():
            committee = result['committee']
            # Create a key from committee member IDs
            committee_key = tuple(sorted([member['ID'] for member in committee]))
            committee_groups[committee_key].append(node_num)
            
        analysis = {
            "total_nodes_with_results": len(self.committee_results),
            "unique_committees": len(committee_groups),
            "committee_groups": {str(k): v for k, v in committee_groups.items()},
            "consensus": len(committee_groups) == 1
        }
        
        return analysis
        
    def generate_report(self):
        """Generate comprehensive analysis report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_nodes": len(self.node_to_id),
            "graph_analysis": {},
            "committee_analysis": {},
            "node_details": {}
        }
        
        # Graph analysis
        one_directional = self.find_one_directional_edges()
        diameter = self.calculate_diameter()
        
        report["graph_analysis"] = {
            "total_edges": sum(len(neighbors) for neighbors in self.neighbors.values()),
            "one_directional_edges": [list(edge) for edge in one_directional],
            "one_directional_count": len(one_directional),
            "diameter": diameter,
            "is_symmetric": len(one_directional) == 0
        }
        
        # Committee analysis
        committee_analysis = self.analyze_committee_consensus()
        report["committee_analysis"] = committee_analysis
        
        # Node details
        for node_num in self.node_to_id:
            neighbors_list = list(self.neighbors[node_num])
            report["node_details"][node_num] = {
                "node_id": self.node_to_id[node_num],
                "neighbors": neighbors_list,
                "neighbor_count": len(neighbors_list),
                "has_committee_result": node_num in self.committee_results
            }
            
        return report
        
    def visualize_graph(self, output_path):
        """Create a visualization of the network graph"""
        G = nx.Graph()
        
        # Add nodes
        for node_num in self.node_to_id:
            G.add_node(node_num, label=f"Node {node_num}")
            
        # Add edges
        for node, neighbors in self.neighbors.items():
            for neighbor in neighbors:
                G.add_edge(node, neighbor)
                
        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(G, k=2, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_color='lightblue', 
                              node_size=800, alpha=0.8)
        
        # Draw edges
        nx.draw_networkx_edges(G, pos, alpha=0.5, width=1)
        
        # Draw labels
        labels = {node: f"N{node}" for node in G.nodes()}
        nx.draw_networkx_labels(G, pos, labels, font_size=10)
        
        plt.title("Network Graph Structure")
        plt.axis('off')
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
    def run_analysis(self):
        """Run complete analysis"""
        print("Starting log analysis...")
        
        # Parse all log files
        for filename in os.listdir(self.log_dir):
            if filename.endswith('.log'):
                filepath = os.path.join(self.log_dir, filename)
                print(f"Processing {filename}...")
                self.parse_log_file(filepath)
                
        # Build graph
        print("Building graph...")
        self.build_graph()
        
        # Generate report
        print("Generating report...")
        report = self.generate_report()
        
        return report

def main():
    log_dir = "../logs-backup-20250713-171809"
    results_dir = "../results"
    
    # Create results directory if it doesn't exist
    os.makedirs(results_dir, exist_ok=True)
    
    # Run analysis
    analyzer = NetworkAnalyzer(log_dir)
    report = analyzer.run_analysis()
    
    # Save report
    report_path = os.path.join(results_dir, "network_analysis_report.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    # Create visualization
    viz_path = os.path.join(results_dir, "network_graph.png")
    analyzer.visualize_graph(viz_path)
    
    # Print summary
    print("\n" + "="*50)
    print("NETWORK ANALYSIS SUMMARY")
    print("="*50)
    print(f"Total nodes: {report['total_nodes']}")
    print(f"Total edges: {report['graph_analysis']['total_edges']}")
    print(f"Graph diameter: {report['graph_analysis']['diameter']}")
    print(f"One-directional edges: {report['graph_analysis']['one_directional_count']}")
    print(f"Is graph symmetric: {report['graph_analysis']['is_symmetric']}")
    
    print("\nCOMMITTEE CONSENSUS ANALYSIS")
    print("-" * 30)
    committee_analysis = report['committee_analysis']
    print(f"Nodes with committee results: {committee_analysis['total_nodes_with_results']}")
    print(f"Unique committees found: {committee_analysis['unique_committees']}")
    print(f"Committee consensus achieved: {committee_analysis['consensus']}")
    
    if committee_analysis['consensus']:
        print("✓ All nodes agree on the same committee")
    else:
        print("✗ Nodes have different committee results")
        
    print(f"\nResults saved to: {results_dir}")
    print(f"Report: {report_path}")
    print(f"Graph visualization: {viz_path}")

if __name__ == "__main__":
    main() 