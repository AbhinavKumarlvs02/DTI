import json
import csv
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
json_file_path = os.path.join(script_dir, 'delhi_metro_graph.json')
csv_output_path = os.path.join(script_dir, 'metro_times_template.csv')

print("[INFO] Loading Metro Graph...")
try:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        graph = json.load(f)
except FileNotFoundError:
    print(f"[ERROR] '{json_file_path}' not found.")
    exit()

unique_edges = {}

# Extract every unique connection
for station_id, data in graph.items():
    station_a = data['name']
    
    for edge in data['connections']:
        station_b = graph[edge['to']]['name']
        edge_type = edge['type']
        
        # Sort alphabetically so A->B and B->A are treated as the same single row
        pair_key = tuple(sorted([station_a, station_b]))
        
        if pair_key not in unique_edges:
            unique_edges[pair_key] = edge_type

print(f"[SUCCESS] Found {len(unique_edges)} unique connections to check.")

# Write to a clean CSV
with open(csv_output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["Station_A", "Station_B", "Type", "Time_in_Minutes"])
    
    for (stn_a, stn_b), e_type in unique_edges.items():
        # Using "FILL_THIS" so you know exactly where to type
        writer.writerow([stn_a, stn_b, e_type.upper(), "FILL_THIS"])

print(f"[SUCCESS] Template created: '{csv_output_path}'")
print("Open this file in Excel, Notepad, or VS Code and replace 'FILL_THIS' with the minutes.")