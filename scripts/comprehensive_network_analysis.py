#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from collections import defaultdict, Counter
import pandas as pd
from matplotlib.patches import Rectangle

def load_network_data():
    """Load and parse network data from the analysis report"""
    with open('../results/network_analysis_report.json', 'r') as f:
        report = json.load(f)
    
    # Create networkx graph
    G = nx.Graph()
    
    # Add nodes with attributes
    for node_num, details in report['node_details'].items():
        node_num = int(node_num)
        G.add_node(node_num, 
                  node_id=details['node_id'],
                  neighbor_count=details['neighbor_count'],
                  has_committee=details['has_committee_result'])
    
    # Add edges
    for node_num, details in report['node_details'].items():
        node_num = int(node_num)
        for neighbor in details['neighbors']:
            G.add_edge(node_num, neighbor)
    
    return G, report

def calculate_comprehensive_metrics(G):
    """Calculate comprehensive network metrics"""
    metrics = {}
    
    # Basic metrics
    metrics['num_nodes'] = G.number_of_nodes()
    metrics['num_edges'] = G.number_of_edges()
    metrics['density'] = nx.density(G)
    
    # Connectivity
    metrics['is_connected'] = nx.is_connected(G)
    metrics['num_components'] = nx.number_connected_components(G)
    
    # Centrality measures
    metrics['degree_centrality'] = nx.degree_centrality(G)
    metrics['betweenness_centrality'] = nx.betweenness_centrality(G)
    metrics['closeness_centrality'] = nx.closeness_centrality(G)
    metrics['eigenvector_centrality'] = nx.eigenvector_centrality(G)
    
    # Clustering
    metrics['clustering_coefficient'] = nx.clustering(G)
    metrics['avg_clustering'] = nx.average_clustering(G)
    metrics['transitivity'] = nx.transitivity(G)
    
    # Path metrics
    if nx.is_connected(G):
        metrics['diameter'] = nx.diameter(G)
        metrics['radius'] = nx.radius(G)
        metrics['avg_path_length'] = nx.average_shortest_path_length(G)
    else:
        metrics['diameter'] = float('inf')
        metrics['radius'] = float('inf')
        metrics['avg_path_length'] = float('inf')
    
    # Degree distribution
    degrees = dict(G.degree())
    metrics['degree_distribution'] = degrees
    metrics['avg_degree'] = np.mean(list(degrees.values()))
    metrics['degree_variance'] = np.var(list(degrees.values()))
    
    return metrics

def create_comprehensive_visualization(G, report, metrics):
    """Create a comprehensive network visualization"""
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    
    # 1. Main network graph
    ax1 = plt.subplot(2, 3, 1)
    create_main_network_plot(G, report, ax1)
    
    # 2. Degree distribution
    ax2 = plt.subplot(2, 3, 2)
    create_degree_distribution_plot(metrics, ax2)
    
    # 3. Centrality comparison
    ax3 = plt.subplot(2, 3, 3)
    create_centrality_plot(metrics, ax3)
    
    # 4. Clustering visualization
    ax4 = plt.subplot(2, 3, 4)
    create_clustering_plot(G, metrics, ax4)
    
    # 5. Committee participation
    ax5 = plt.subplot(2, 3, 5)
    create_committee_participation_plot(G, report, ax5)
    
    # 6. Network metrics summary
    ax6 = plt.subplot(2, 3, 6)
    create_metrics_summary_plot(metrics, ax6)
    
    plt.tight_layout()
    plt.savefig('../results/comprehensive_network_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_main_network_plot(G, report, ax):
    """Create the main network visualization"""
    # Create layout
    pos = nx.spring_layout(G, k=3, iterations=100, seed=42)
    
    # Node colors based on committee participation
    node_colors = []
    for node in G.nodes():
        if G.nodes[node]['has_committee']:
            node_colors.append('lightgreen')
        else:
            node_colors.append('lightcoral')
    
    # Node sizes based on degree
    node_sizes = [G.degree(node) * 100 for node in G.nodes()]
    
    # Draw network
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, 
                          node_size=node_sizes, alpha=0.8, ax=ax)
    
    # Draw edges with varying thickness
    edge_widths = [0.5 + G.degree(u) * G.degree(v) * 0.01 for u, v in G.edges()]
    nx.draw_networkx_edges(G, pos, width=edge_widths, alpha=0.4, ax=ax)
    
    # Draw labels
    labels = {node: f"N{node}" for node in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels, font_size=8, font_weight='bold', ax=ax)
    
    ax.set_title('Complete Network Structure\n(Green: Committee Participants, Red: Non-participants)', 
                fontsize=12, fontweight='bold')
    ax.axis('off')

def create_degree_distribution_plot(metrics, ax):
    """Create degree distribution plot"""
    degrees = list(metrics['degree_distribution'].values())
    degree_counts = Counter(degrees)
    
    x = list(degree_counts.keys())
    y = list(degree_counts.values())
    
    ax.bar(x, y, alpha=0.7, color='skyblue', edgecolor='black')
    ax.set_xlabel('Degree')
    ax.set_ylabel('Number of Nodes')
    ax.set_title('Degree Distribution')
    ax.grid(True, alpha=0.3)
    
    # Add statistics
    ax.text(0.7, 0.9, f'Avg Degree: {metrics["avg_degree"]:.2f}\nVariance: {metrics["degree_variance"]:.2f}',
            transform=ax.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

def create_centrality_plot(metrics, ax):
    """Create centrality comparison plot"""
    nodes = list(metrics['degree_centrality'].keys())
    
    # Prepare data
    centrality_data = {
        'Degree': [metrics['degree_centrality'][node] for node in nodes],
        'Betweenness': [metrics['betweenness_centrality'][node] for node in nodes],
        'Closeness': [metrics['closeness_centrality'][node] for node in nodes],
        'Eigenvector': [metrics['eigenvector_centrality'][node] for node in nodes]
    }
    
    # Create DataFrame for easier plotting
    df = pd.DataFrame(centrality_data, index=[f'N{node}' for node in nodes])
    
    # Create heatmap
    im = ax.imshow(df.T.values, cmap='YlOrRd', aspect='auto')
    
    # Set labels
    ax.set_xticks(range(len(nodes)))
    ax.set_xticklabels([f'N{node}' for node in nodes], rotation=45)
    ax.set_yticks(range(len(centrality_data)))
    ax.set_yticklabels(centrality_data.keys())
    
    # Add colorbar
    plt.colorbar(im, ax=ax, shrink=0.8)
    
    ax.set_title('Centrality Measures Heatmap')

def create_clustering_plot(G, metrics, ax):
    """Create clustering visualization"""
    # Get clustering coefficients
    clustering = metrics['clustering_coefficient']
    nodes = list(clustering.keys())
    values = list(clustering.values())
    
    # Create scatter plot
    degrees = [G.degree(node) for node in nodes]
    colors = ['red' if G.nodes[node]['has_committee'] else 'blue' for node in nodes]
    
    scatter = ax.scatter(degrees, values, c=colors, alpha=0.7, s=100)
    
    # Add labels for interesting points
    for i, node in enumerate(nodes):
        if values[i] > 0.5 or degrees[i] > 6:
            ax.annotate(f'N{node}', (degrees[i], values[i]), 
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
    
    ax.set_xlabel('Node Degree')
    ax.set_ylabel('Clustering Coefficient')
    ax.set_title('Clustering vs Degree\n(Red: Committee, Blue: Non-committee)')
    ax.grid(True, alpha=0.3)
    
    # Add average clustering line
    ax.axhline(y=metrics['avg_clustering'], color='green', linestyle='--', 
               label=f'Avg Clustering: {metrics["avg_clustering"]:.3f}')
    ax.legend()

def create_committee_participation_plot(G, report, ax):
    """Create committee participation analysis"""
    committee_counts = defaultdict(int)
    
    # Count committee participation
    for node_num, details in report['node_details'].items():
        if details['has_committee_result']:
            committee_counts['Participated'] += 1
        else:
            committee_counts['Did Not Participate'] += 1
    
    # Create pie chart
    labels = list(committee_counts.keys())
    sizes = list(committee_counts.values())
    colors = ['lightgreen', 'lightcoral']
    
    wedges, texts, autotexts = ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%',
                                      startangle=90, textprops={'fontsize': 10})
    
    ax.set_title('Committee Election Participation')
    
    # Add committee consensus information
    committee_analysis = report['committee_analysis']
    consensus_text = f"Consensus: {'Yes' if committee_analysis['consensus'] else 'No'}\n"
    consensus_text += f"Unique Committees: {committee_analysis['unique_committees']}"
    
    ax.text(0.02, 0.02, consensus_text, transform=ax.transAxes, 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
            verticalalignment='bottom')

def create_metrics_summary_plot(metrics, ax):
    """Create network metrics summary"""
    ax.axis('off')
    
    # Prepare metrics text
    diameter_str = f"{metrics['diameter']}" if metrics['diameter'] != float('inf') else 'N/A'
    radius_str = f"{metrics['radius']}" if metrics['radius'] != float('inf') else 'N/A'
    path_length_str = f"{metrics['avg_path_length']:.2f}" if metrics['avg_path_length'] != float('inf') else 'N/A'
    
    summary_text = f"""NETWORK METRICS SUMMARY

Basic Properties:
• Nodes: {metrics['num_nodes']}
• Edges: {metrics['num_edges']}
• Density: {metrics['density']:.3f}
• Connected: {metrics['is_connected']}
• Components: {metrics['num_components']}

Centralization:
• Avg Degree: {metrics['avg_degree']:.2f}
• Degree Variance: {metrics['degree_variance']:.2f}

Clustering:
• Avg Clustering: {metrics['avg_clustering']:.3f}
• Transitivity: {metrics['transitivity']:.3f}

Path Properties:
• Diameter: {diameter_str}
• Radius: {radius_str}
• Avg Path Length: {path_length_str}
"""
    
    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, 
            verticalalignment='top', fontfamily='monospace', fontsize=10,
            bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

def create_detailed_network_visualization(G, report):
    """Create a detailed network visualization with multiple views"""
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Force-directed layout
    pos1 = nx.spring_layout(G, k=2, iterations=100, seed=42)
    draw_network_with_labels(G, pos1, ax1, "Force-Directed Layout")
    
    # 2. Circular layout
    pos2 = nx.circular_layout(G)
    draw_network_with_labels(G, pos2, ax2, "Circular Layout")
    
    # 3. Degree-based layout
    pos3 = create_degree_based_layout(G)
    draw_network_with_labels(G, pos3, ax3, "Degree-Based Layout")
    
    # 4. Committee participation layout
    pos4 = create_committee_layout(G, report)
    draw_network_with_committee_colors(G, pos4, ax4, "Committee Participation Layout")
    
    plt.tight_layout()
    plt.savefig('../results/detailed_network_visualizations.png', dpi=300, bbox_inches='tight')
    plt.close()

def draw_network_with_labels(G, pos, ax, title):
    """Draw network with consistent styling"""
    # Node colors based on degree
    degrees = dict(G.degree())
    max_degree = max(degrees.values())
    node_colors = [degrees[node] / max_degree for node in G.nodes()]
    
    # Node sizes based on degree
    node_sizes = [degrees[node] * 80 for node in G.nodes()]
    
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, 
                          node_size=node_sizes, cmap='viridis', alpha=0.8, ax=ax)
    nx.draw_networkx_edges(G, pos, alpha=0.3, ax=ax)
    nx.draw_networkx_labels(G, pos, {node: f"N{node}" for node in G.nodes()}, 
                           font_size=8, font_weight='bold', ax=ax)
    
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.axis('off')

def draw_network_with_committee_colors(G, pos, ax, title):
    """Draw network with committee participation colors"""
    node_colors = []
    for node in G.nodes():
        if G.nodes[node]['has_committee']:
            node_colors.append('lightgreen')
        else:
            node_colors.append('lightcoral')
    
    node_sizes = [G.degree(node) * 80 for node in G.nodes()]
    
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, 
                          node_size=node_sizes, alpha=0.8, ax=ax)
    nx.draw_networkx_edges(G, pos, alpha=0.3, ax=ax)
    nx.draw_networkx_labels(G, pos, {node: f"N{node}" for node in G.nodes()}, 
                           font_size=8, font_weight='bold', ax=ax)
    
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.axis('off')

def create_degree_based_layout(G):
    """Create layout based on node degrees"""
    degrees = dict(G.degree())
    pos = {}
    
    # Sort nodes by degree
    sorted_nodes = sorted(degrees.keys(), key=lambda x: degrees[x], reverse=True)
    
    # Create concentric circles based on degree
    max_degree = max(degrees.values())
    num_circles = max_degree // 2 + 1
    
    circle_nodes = [[] for _ in range(num_circles)]
    
    for node in sorted_nodes:
        circle_idx = min(degrees[node] // 2, num_circles - 1)
        circle_nodes[circle_idx].append(node)
    
    # Position nodes in circles
    for circle_idx, nodes in enumerate(circle_nodes):
        if not nodes:
            continue
        
        radius = (circle_idx + 1) * 0.5
        angle_step = 2 * np.pi / len(nodes)
        
        for i, node in enumerate(nodes):
            angle = i * angle_step
            pos[node] = (radius * np.cos(angle), radius * np.sin(angle))
    
    return pos

def create_committee_layout(G, report):
    """Create layout separating committee participants"""
    pos = {}
    
    # Separate nodes by committee participation
    committee_nodes = []
    non_committee_nodes = []
    
    for node in G.nodes():
        if G.nodes[node]['has_committee']:
            committee_nodes.append(node)
        else:
            non_committee_nodes.append(node)
    
    # Position committee nodes on the left
    if committee_nodes:
        committee_pos = nx.spring_layout(G.subgraph(committee_nodes), center=(-1, 0), k=1)
        pos.update(committee_pos)
    
    # Position non-committee nodes on the right
    if non_committee_nodes:
        non_committee_pos = nx.spring_layout(G.subgraph(non_committee_nodes), center=(1, 0), k=1)
        pos.update(non_committee_pos)
    
    return pos

def generate_comprehensive_report(G, report, metrics):
    """Generate a comprehensive text report"""
    # Prepare conditional strings
    diameter_str = f"{metrics['diameter']}" if metrics['diameter'] != float('inf') else 'N/A (disconnected)'
    radius_str = f"{metrics['radius']}" if metrics['radius'] != float('inf') else 'N/A (disconnected)'
    path_length_str = f"{metrics['avg_path_length']:.2f}" if metrics['avg_path_length'] != float('inf') else 'N/A (disconnected)'
    
    # Get top centrality nodes
    top_degree = sorted(metrics['degree_centrality'].items(), key=lambda x: x[1], reverse=True)[:5]
    top_betweenness = sorted(metrics['betweenness_centrality'].items(), key=lambda x: x[1], reverse=True)[:5]
    top_closeness = sorted(metrics['closeness_centrality'].items(), key=lambda x: x[1], reverse=True)[:5]
    top_clustering = sorted(metrics['clustering_coefficient'].items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Get degree distribution
    degree_dist = dict(Counter(metrics['degree_distribution'].values()))
    
    # Calculate asymmetric percentage
    asymmetric_pct = report['graph_analysis']['one_directional_count'] / metrics['num_edges'] * 100
    
    # Health assessments
    connectivity_health = 'Good' if metrics['is_connected'] else 'Poor - Disconnected'
    density_health = 'Low' if metrics['density'] < 0.3 else 'Medium' if metrics['density'] < 0.7 else 'High'
    clustering_health = 'Low' if metrics['avg_clustering'] < 0.3 else 'Medium' if metrics['avg_clustering'] < 0.7 else 'High'
    consensus_health = 'Failed' if not report['committee_analysis']['consensus'] else 'Achieved'
    
    report_text = f"""COMPREHENSIVE NETWORK ANALYSIS REPORT
=====================================

NETWORK STRUCTURE:
• Total Nodes: {metrics['num_nodes']}
• Total Edges: {metrics['num_edges']}
• Network Density: {metrics['density']:.3f}
• Is Connected: {metrics['is_connected']}
• Connected Components: {metrics['num_components']}

CENTRALITY ANALYSIS:
• Most central nodes (degree): {top_degree}
• Most central nodes (betweenness): {top_betweenness}
• Most central nodes (closeness): {top_closeness}

CLUSTERING ANALYSIS:
• Average Clustering Coefficient: {metrics['avg_clustering']:.3f}
• Global Clustering (Transitivity): {metrics['transitivity']:.3f}
• Nodes with highest clustering: {top_clustering}

PATH ANALYSIS:
• Network Diameter: {diameter_str}
• Network Radius: {radius_str}
• Average Shortest Path Length: {path_length_str}

DEGREE DISTRIBUTION:
• Average Degree: {metrics['avg_degree']:.2f}
• Degree Variance: {metrics['degree_variance']:.2f}
• Degree Distribution: {degree_dist}

COMMITTEE ANALYSIS:
• Committee Participants: {report['committee_analysis']['total_nodes_with_results']}/{metrics['num_nodes']}
• Committee Consensus: {report['committee_analysis']['consensus']}
• Unique Committees: {report['committee_analysis']['unique_committees']}

ASYMMETRIC EDGES:
• One-directional edges: {report['graph_analysis']['one_directional_count']}
• Percentage asymmetric: {asymmetric_pct:.1f}%
• Graph symmetry: {report['graph_analysis']['is_symmetric']}

NETWORK HEALTH ASSESSMENT:
• Connectivity: {connectivity_health}
• Density: {density_health}
• Clustering: {clustering_health}
• Committee Consensus: {consensus_health}
"""
    
    return report_text

def main():
    """Main analysis function"""
    print("Loading network data...")
    G, report = load_network_data()
    
    print("Calculating comprehensive metrics...")
    metrics = calculate_comprehensive_metrics(G)
    
    print("Creating comprehensive visualization...")
    create_comprehensive_visualization(G, report, metrics)
    
    print("Creating detailed network visualizations...")
    create_detailed_network_visualization(G, report)
    
    print("Generating comprehensive report...")
    comprehensive_report = generate_comprehensive_report(G, report, metrics)
    
    # Save comprehensive report
    with open('../results/comprehensive_analysis_report.txt', 'w') as f:
        f.write(comprehensive_report)
    
    # Print summary to console
    print("\n" + "="*60)
    print("COMPREHENSIVE NETWORK ANALYSIS COMPLETE")
    print("="*60)
    print(comprehensive_report)
    
    print("\nFiles generated:")
    print("• comprehensive_network_analysis.png - Multi-panel analysis")
    print("• detailed_network_visualizations.png - Multiple layout views")
    print("• comprehensive_analysis_report.txt - Detailed text report")

if __name__ == "__main__":
    main() 