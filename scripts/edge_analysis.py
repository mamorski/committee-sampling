#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import networkx as nx
from collections import defaultdict

def analyze_one_directional_edges():
    # Load the analysis report
    with open('../results/network_analysis_report.json', 'r') as f:
        report = json.load(f)
    
    one_directional = report['graph_analysis']['one_directional_edges']
    node_details = report['node_details']
    
    print("ONE-DIRECTIONAL EDGES DETAILED ANALYSIS")
    print("=" * 50)
    
    # Count outgoing and incoming one-directional edges per node
    outgoing_count = defaultdict(int)
    incoming_count = defaultdict(int)
    
    for edge in one_directional:
        source, target = edge
        outgoing_count[source] += 1
        incoming_count[target] += 1
    
    print(f"Total one-directional edges: {len(one_directional)}")
    print(f"Percentage of asymmetric edges: {len(one_directional) / report['graph_analysis']['total_edges'] * 100:.1f}%")
    print()
    
    # Most problematic nodes
    print("NODES WITH MOST OUTGOING ONE-DIRECTIONAL EDGES:")
    for node, count in sorted(outgoing_count.items(), key=lambda x: x[1], reverse=True):
        node_id = node_details[str(node)]['node_id']
        print(f"  Node {node}: {count} outgoing one-directional edges")
        print(f"    Node ID: {node_id}")
        print(f"    Total neighbors: {node_details[str(node)]['neighbor_count']}")
        print()
    
    print("NODES WITH MOST INCOMING ONE-DIRECTIONAL EDGES:")
    for node, count in sorted(incoming_count.items(), key=lambda x: x[1], reverse=True):
        node_id = node_details[str(node)]['node_id']
        print(f"  Node {node}: {count} incoming one-directional edges")
        print(f"    Node ID: {node_id}")
        print(f"    Total neighbors: {node_details[str(node)]['neighbor_count']}")
        print()
    
    # Detailed edge analysis
    print("DETAILED ONE-DIRECTIONAL EDGE ANALYSIS:")
    print("-" * 40)
    for i, edge in enumerate(one_directional, 1):
        source, target = edge
        source_id = node_details[str(source)]['node_id']
        target_id = node_details[str(target)]['node_id']
        
        print(f"{i}. Node {source} → Node {target}")
        print(f"   Source ID: {source_id}")
        print(f"   Target ID: {target_id}")
        print(f"   Source neighbors: {node_details[str(source)]['neighbor_count']}")
        print(f"   Target neighbors: {node_details[str(target)]['neighbor_count']}")
        print()
    
    # Create visualization focused on one-directional edges
    create_asymmetric_graph_visualization(report, one_directional)

def create_asymmetric_graph_visualization(report, one_directional):
    """Create a visualization highlighting asymmetric edges"""
    G = nx.DiGraph()  # Use directed graph
    
    # Add all nodes
    for node_num in report['node_details']:
        node_num = int(node_num)
        G.add_node(node_num)
    
    # Add all edges from the neighbor data
    for node_num, details in report['node_details'].items():
        node_num = int(node_num)
        for neighbor in details['neighbors']:
            G.add_edge(node_num, neighbor)
    
    plt.figure(figsize=(14, 10))
    
    # Create layout
    pos = nx.spring_layout(G, k=3, iterations=50)
    
    # Draw all nodes
    nx.draw_networkx_nodes(G, pos, node_color='lightblue', 
                          node_size=1000, alpha=0.8)
    
    # Draw symmetric edges in light gray
    symmetric_edges = []
    asymmetric_edges = []
    
    for edge in G.edges():
        source, target = edge
        reverse_edge = (target, source)
        
        if reverse_edge in G.edges():
            # This is a symmetric edge
            symmetric_edges.append(edge)
        else:
            # This is an asymmetric edge
            asymmetric_edges.append(edge)
    
    # Draw symmetric edges
    nx.draw_networkx_edges(G, pos, edgelist=symmetric_edges, 
                          edge_color='lightgray', alpha=0.3, width=1)
    
    # Draw asymmetric edges in red
    nx.draw_networkx_edges(G, pos, edgelist=asymmetric_edges, 
                          edge_color='red', alpha=0.8, width=2, 
                          arrowsize=20, arrowstyle='->')
    
    # Draw labels
    labels = {node: f"N{node}" for node in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels, font_size=10, font_weight='bold')
    
    plt.title("Network Graph - One-Directional Edges Highlighted in Red", fontsize=16)
    plt.text(0.02, 0.98, f"Total edges: {len(G.edges())}\nAsymmetric edges: {len(asymmetric_edges)}\nSymmetric edges: {len(symmetric_edges)}", 
             transform=plt.gca().transAxes, fontsize=12, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.axis('off')
    plt.tight_layout()
    plt.savefig('../results/asymmetric_edges_visualization.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Asymmetric edges visualization saved to: ../results/asymmetric_edges_visualization.png")

if __name__ == "__main__":
    analyze_one_directional_edges() 