#!/usr/bin/env python3

import json
import re
import os
from collections import defaultdict, Counter
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

class AsymmetricEdgeInvestigator:
    def __init__(self, log_dir, results_dir):
        self.log_dir = log_dir
        self.results_dir = results_dir
        self.node_to_id = {}
        self.id_to_node = {}
        self.connection_attempts = defaultdict(list)
        self.successful_connections = defaultdict(list)
        self.failed_connections = defaultdict(list)
        self.negotiation_requests = defaultdict(list)
        self.neighbor_additions = defaultdict(list)
        self.connection_timeline = []
        
    def extract_node_id(self, log_filename):
        """Extract node number from log filename"""
        match = re.search(r'node-(\d+)-log\.log', log_filename)
        return int(match.group(1)) if match else None
        
    def parse_timestamp(self, timestamp_str):
        """Parse timestamp string to datetime object"""
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except:
            return None
            
    def investigate_logs(self):
        """Parse all log files to understand connection patterns"""
        print("Investigating connection patterns in logs...")
        
        for filename in os.listdir(self.log_dir):
            if filename.endswith('.log'):
                filepath = os.path.join(self.log_dir, filename)
                node_num = self.extract_node_id(filename)
                if node_num:
                    self.parse_log_file(filepath, node_num)
                    
    def parse_log_file(self, filepath, node_num):
        """Parse a single log file for connection information"""
        print(f"  Analyzing {os.path.basename(filepath)}...")
        
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or not line.startswith('{'):
                    continue
                    
                try:
                    log_entry = json.loads(line)
                    self.analyze_log_entry(log_entry, node_num, line_num)
                except json.JSONDecodeError:
                    continue
                    
    def analyze_log_entry(self, log_entry, node_num, line_num):
        """Analyze a single log entry for connection information"""
        msg = log_entry.get('msg', '')
        timestamp = self.parse_timestamp(log_entry.get('timestamp', ''))
        node_id = log_entry.get('node_id')
        peer_id = log_entry.get('peer_id')
        
        # Map node IDs to numbers
        if node_id:
            self.node_to_id[node_num] = node_id
            self.id_to_node[node_id] = node_num
            
        # Track various connection events
        entry_data = {
            'timestamp': timestamp,
            'node_num': node_num,
            'node_id': node_id,
            'peer_id': peer_id,
            'line_num': line_num,
            'log_entry': log_entry
        }
        
        if msg == "Attempting to connect to discovered peer":
            self.connection_attempts[node_num].append(entry_data)
            self.connection_timeline.append(('attempt', entry_data))
            
        elif msg == "Successfully sent neighbor request":
            self.successful_connections[node_num].append(entry_data)
            self.connection_timeline.append(('request_sent', entry_data))
            
        elif msg == "Received negotiation request":
            self.negotiation_requests[node_num].append(entry_data)
            self.connection_timeline.append(('negotiation_received', entry_data))
            
        elif msg == "Successfully added neighbor":
            self.neighbor_additions[node_num].append(entry_data)
            self.connection_timeline.append(('neighbor_added', entry_data))
            
        elif "failed" in msg.lower() or "error" in msg.lower():
            self.failed_connections[node_num].append(entry_data)
            self.connection_timeline.append(('failed', entry_data))
            
    def analyze_asymmetric_patterns(self):
        """Analyze patterns in asymmetric edge formation"""
        print("\nAnalyzing asymmetric edge patterns...")
        
        # Load previous analysis results
        with open(os.path.join(self.results_dir, 'network_analysis_report.json'), 'r') as f:
            report = json.load(f)
            
        asymmetric_edges = report['graph_analysis']['one_directional_edges']
        
        analysis = {
            'asymmetric_edges': asymmetric_edges,
            'connection_analysis': {},
            'timing_analysis': {},
            'failure_analysis': {}
        }
        
        for edge in asymmetric_edges:
            source_node, target_node = edge
            analysis['connection_analysis'][f"{source_node}->{target_node}"] = \
                self.analyze_specific_edge(source_node, target_node)
                
        return analysis
        
    def analyze_specific_edge(self, source_node, target_node):
        """Analyze a specific asymmetric edge in detail"""
        source_id = self.node_to_id.get(source_node)
        target_id = self.node_to_id.get(target_node)
        
        if not source_id or not target_id:
            return {"error": "Node IDs not found"}
            
        analysis = {
            'source_node': source_node,
            'target_node': target_node,
            'source_id': source_id,
            'target_id': target_id,
            'connection_attempts': [],
            'negotiations': [],
            'successful_additions': [],
            'failures': [],
            'timing_issues': []
        }
        
        # Check source node's attempts to connect to target
        for attempt in self.connection_attempts[source_node]:
            if attempt['peer_id'] == target_id:
                analysis['connection_attempts'].append({
                    'timestamp': attempt['timestamp'].isoformat() if attempt['timestamp'] else None,
                    'direction': f"{source_node} -> {target_node}",
                    'details': attempt['log_entry']
                })
                
        # Check if target received negotiation from source
        for negotiation in self.negotiation_requests[target_node]:
            if negotiation['log_entry'].get('NodeId') == source_id:
                analysis['negotiations'].append({
                    'timestamp': negotiation['timestamp'].isoformat() if negotiation['timestamp'] else None,
                    'direction': f"{source_node} -> {target_node}",
                    'details': negotiation['log_entry']
                })
                
        # Check successful neighbor additions
        for addition in self.neighbor_additions[source_node]:
            if addition['peer_id'] == target_id:
                analysis['successful_additions'].append({
                    'timestamp': addition['timestamp'].isoformat() if addition['timestamp'] else None,
                    'direction': f"{source_node} -> {target_node}",
                    'node': source_node,
                    'details': addition['log_entry']
                })
                
        for addition in self.neighbor_additions[target_node]:
            if addition['peer_id'] == source_id:
                analysis['successful_additions'].append({
                    'timestamp': addition['timestamp'].isoformat() if addition['timestamp'] else None,
                    'direction': f"{target_node} -> {source_node}",
                    'node': target_node,
                    'details': addition['log_entry']
                })
                
        # Check for failures
        for failure in self.failed_connections[source_node]:
            if failure['peer_id'] == target_id:
                analysis['failures'].append({
                    'timestamp': failure['timestamp'].isoformat() if failure['timestamp'] else None,
                    'node': source_node,
                    'details': failure['log_entry']
                })
                
        for failure in self.failed_connections[target_node]:
            if failure['peer_id'] == source_id:
                analysis['failures'].append({
                    'timestamp': failure['timestamp'].isoformat() if failure['timestamp'] else None,
                    'node': target_node,
                    'details': failure['log_entry']
                })
                
        return analysis
        
    def analyze_connection_timing(self):
        """Analyze timing patterns in connections"""
        print("Analyzing connection timing patterns...")
        
        # Sort timeline by timestamp
        valid_timeline = [entry for entry in self.connection_timeline 
                         if entry[1]['timestamp'] is not None]
        valid_timeline.sort(key=lambda x: x[1]['timestamp'])
        
        timing_analysis = {
            'total_events': len(valid_timeline),
            'event_types': Counter([entry[0] for entry in valid_timeline]),
            'time_gaps': [],
            'simultaneous_events': [],
            'problematic_sequences': []
        }
        
        # Analyze time gaps between events
        for i in range(1, len(valid_timeline)):
            prev_event = valid_timeline[i-1]
            curr_event = valid_timeline[i]
            
            time_gap = (curr_event[1]['timestamp'] - prev_event[1]['timestamp']).total_seconds()
            timing_analysis['time_gaps'].append(time_gap)
            
            # Look for very fast sequences that might indicate race conditions
            if time_gap < 0.1:  # Less than 100ms
                timing_analysis['simultaneous_events'].append({
                    'gap_seconds': time_gap,
                    'prev_event': prev_event[0],
                    'curr_event': curr_event[0],
                    'prev_node': prev_event[1]['node_num'],
                    'curr_node': curr_event[1]['node_num']
                })
                
        return timing_analysis
        
    def check_address_patterns(self):
        """Check for address-related patterns in asymmetric edges"""
        print("Checking address patterns...")
        
        address_analysis = {
            'node_addresses': {},
            'connection_addresses': defaultdict(list),
            'nat_indicators': [],
            'firewall_indicators': []
        }
        
        # Look for address information in logs
        for node_num in self.node_to_id.keys():
            # Check connection attempts for address info
            for attempt in self.connection_attempts[node_num]:
                addresses = attempt['log_entry'].get('addresses', [])
                peer_id = attempt['peer_id']
                
                if addresses:
                    address_analysis['connection_addresses'][node_num].extend([
                        {'peer_id': peer_id, 'addresses': addresses, 'timestamp': attempt['timestamp']}
                    ])
                    
                    # Look for NAT indicators (multiple IPs, 127.0.0.1 vs external)
                    if len(addresses) > 1:
                        localhost_count = sum(1 for addr in addresses if '127.0.0.1' in addr)
                        external_count = len(addresses) - localhost_count
                        
                        if localhost_count > 0 and external_count > 0:
                            address_analysis['nat_indicators'].append({
                                'node': node_num,
                                'peer_id': peer_id,
                                'addresses': addresses,
                                'localhost_count': localhost_count,
                                'external_count': external_count
                            })
                            
        return address_analysis
        
    def generate_investigation_report(self, asymmetric_analysis, timing_analysis, address_analysis):
        """Generate comprehensive investigation report"""
        report = {
            'investigation_summary': {
                'total_asymmetric_edges': len(asymmetric_analysis['asymmetric_edges']),
                'investigation_timestamp': datetime.now().isoformat(),
                'key_findings': []
            },
            'asymmetric_edge_details': asymmetric_analysis,
            'timing_analysis': timing_analysis,
            'address_analysis': address_analysis,
            'root_cause_analysis': self.determine_root_causes(asymmetric_analysis, timing_analysis, address_analysis)
        }
        
        return report
        
    def determine_root_causes(self, asymmetric_analysis, timing_analysis, address_analysis):
        """Determine likely root causes of asymmetric edges"""
        root_causes = {
            'likely_causes': [],
            'evidence': {},
            'confidence_scores': {}
        }
        
        # Check for timing-related issues
        simultaneous_events = len(timing_analysis['simultaneous_events'])
        if simultaneous_events > 10:
            root_causes['likely_causes'].append('Race conditions in connection establishment')
            root_causes['evidence']['race_conditions'] = f"{simultaneous_events} events within 100ms of each other"
            root_causes['confidence_scores']['race_conditions'] = min(simultaneous_events / 20.0, 1.0)
            
        # Check for NAT issues
        nat_indicators = len(address_analysis['nat_indicators'])
        if nat_indicators > 0:
            root_causes['likely_causes'].append('NAT traversal issues')
            root_causes['evidence']['nat_issues'] = f"{nat_indicators} connections with mixed localhost/external addresses"
            root_causes['confidence_scores']['nat_issues'] = min(nat_indicators / 5.0, 1.0)
            
        # Check for one-sided successful connections
        one_sided_success = 0
        for edge_key, edge_analysis in asymmetric_analysis['connection_analysis'].items():
            if isinstance(edge_analysis, dict):
                additions = edge_analysis.get('successful_additions', [])
                source_additions = sum(1 for add in additions if add['direction'].startswith(edge_key.split('->')[0]))
                target_additions = sum(1 for add in additions if add['direction'].startswith(edge_key.split('->')[1]))
                
                if source_additions > 0 and target_additions == 0:
                    one_sided_success += 1
                    
        if one_sided_success > 0:
            root_causes['likely_causes'].append('Asymmetric connection acceptance')
            root_causes['evidence']['one_sided_success'] = f"{one_sided_success} connections successful in only one direction"
            root_causes['confidence_scores']['one_sided_success'] = min(one_sided_success / 3.0, 1.0)
            
        # Check for discovery timing issues
        if len(timing_analysis['time_gaps']) > 0:
            avg_gap = sum(timing_analysis['time_gaps']) / len(timing_analysis['time_gaps'])
            if avg_gap < 1.0:  # Very fast discovery might cause issues
                root_causes['likely_causes'].append('Rapid peer discovery causing connection conflicts')
                root_causes['evidence']['rapid_discovery'] = f"Average time gap between events: {avg_gap:.3f}s"
                root_causes['confidence_scores']['rapid_discovery'] = max(0, 1.0 - avg_gap)
                
        return root_causes
        
    def create_investigation_visualizations(self, asymmetric_analysis, timing_analysis):
        """Create visualizations for the investigation"""
        print("Creating investigation visualizations...")
        
        # Create timeline visualization
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Event timeline
        self.plot_event_timeline(timing_analysis, ax1)
        
        # 2. Time gap distribution
        self.plot_time_gaps(timing_analysis, ax2)
        
        # 3. Asymmetric edge patterns
        self.plot_asymmetric_patterns(asymmetric_analysis, ax3)
        
        # 4. Connection success rates
        self.plot_connection_success_rates(asymmetric_analysis, ax4)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.results_dir, 'asymmetric_edge_investigation.png'), 
                   dpi=300, bbox_inches='tight')
        plt.close()
        
    def plot_event_timeline(self, timing_analysis, ax):
        """Plot event timeline"""
        event_types = list(timing_analysis['event_types'].keys())
        event_counts = list(timing_analysis['event_types'].values())
        
        ax.bar(event_types, event_counts, alpha=0.7)
        ax.set_title('Connection Events Distribution')
        ax.set_ylabel('Count')
        ax.tick_params(axis='x', rotation=45)
        
    def plot_time_gaps(self, timing_analysis, ax):
        """Plot time gap distribution"""
        if timing_analysis['time_gaps']:
            ax.hist(timing_analysis['time_gaps'], bins=30, alpha=0.7, edgecolor='black')
            ax.set_title('Time Gaps Between Events')
            ax.set_xlabel('Seconds')
            ax.set_ylabel('Frequency')
            ax.axvline(x=0.1, color='red', linestyle='--', label='100ms threshold')
            ax.legend()
        else:
            ax.text(0.5, 0.5, 'No timing data available', ha='center', va='center', transform=ax.transAxes)
            ax.set_title('Time Gaps Between Events')
            
    def plot_asymmetric_patterns(self, asymmetric_analysis, ax):
        """Plot asymmetric edge patterns"""
        edges = asymmetric_analysis['asymmetric_edges']
        edge_labels = [f"N{edge[0]}→N{edge[1]}" for edge in edges]
        
        # Count successful connections per edge
        success_counts = []
        for edge in edges:
            source, target = edge
            edge_key = f"{source}->{target}"
            edge_analysis = asymmetric_analysis['connection_analysis'].get(edge_key, {})
            if isinstance(edge_analysis, dict):
                success_counts.append(len(edge_analysis.get('successful_additions', [])))
            else:
                success_counts.append(0)
                
        ax.bar(range(len(edges)), success_counts, alpha=0.7)
        ax.set_title('Successful Connections per Asymmetric Edge')
        ax.set_xlabel('Asymmetric Edge')
        ax.set_ylabel('Successful Connections')
        ax.set_xticks(range(len(edges)))
        ax.set_xticklabels(edge_labels, rotation=45)
        
    def plot_connection_success_rates(self, asymmetric_analysis, ax):
        """Plot connection success rates"""
        # This is a placeholder - would need more detailed analysis
        ax.text(0.5, 0.5, 'Connection Success Rate Analysis\n(Requires deeper log analysis)', 
               ha='center', va='center', transform=ax.transAxes)
        ax.set_title('Connection Success Rates')
        
    def run_investigation(self):
        """Run the complete investigation"""
        print("Starting asymmetric edge investigation...")
        
        # Parse logs
        self.investigate_logs()
        
        # Analyze patterns
        asymmetric_analysis = self.analyze_asymmetric_patterns()
        timing_analysis = self.analyze_connection_timing()
        address_analysis = self.check_address_patterns()
        
        # Generate report
        investigation_report = self.generate_investigation_report(
            asymmetric_analysis, timing_analysis, address_analysis)
        
        # Save report
        report_path = os.path.join(self.results_dir, 'asymmetric_edge_investigation.json')
        with open(report_path, 'w') as f:
            json.dump(investigation_report, f, indent=2, default=str)
            
        # Create visualizations
        self.create_investigation_visualizations(asymmetric_analysis, timing_analysis)
        
        # Print summary
        self.print_investigation_summary(investigation_report)
        
        print(f"\nInvestigation complete. Results saved to: {report_path}")
        
    def print_investigation_summary(self, report):
        """Print investigation summary"""
        print("\n" + "="*60)
        print("ASYMMETRIC EDGE INVESTIGATION SUMMARY")
        print("="*60)
        
        summary = report['investigation_summary']
        root_causes = report['root_cause_analysis']
        
        print(f"Total asymmetric edges investigated: {summary['total_asymmetric_edges']}")
        
        print("\nLIKELY ROOT CAUSES:")
        for cause in root_causes['likely_causes']:
            confidence = max(root_causes['confidence_scores'].get(
                cause.lower().replace(' ', '_').replace('/', '_'), 0), 0)
            print(f"• {cause} (Confidence: {confidence:.1%})")
            
        print("\nEVIDENCE:")
        for evidence_type, evidence in root_causes['evidence'].items():
            print(f"• {evidence_type}: {evidence}")
            
        timing = report['timing_analysis']
        print(f"\nTIMING ANALYSIS:")
        print(f"• Total events analyzed: {timing['total_events']}")
        print(f"• Simultaneous events (<100ms): {len(timing['simultaneous_events'])}")
        
        addresses = report['address_analysis']
        print(f"\nADDRESS ANALYSIS:")
        print(f"• NAT indicators found: {len(addresses['nat_indicators'])}")

def main():
    log_dir = "../logs-backup-20250713-171809"
    results_dir = "../results"
    
    investigator = AsymmetricEdgeInvestigator(log_dir, results_dir)
    investigator.run_investigation()

if __name__ == "__main__":
    main() 