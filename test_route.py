import json
import heapq
import sys
import os

# 1. Safely resolve the file path relative to where the script is running
script_dir = os.path.dirname(os.path.abspath(__file__))
json_file_path = os.path.join(script_dir, 'delhi_metro_graph.json')

print("--- INITIALIZING ROUTING ENGINE ---")

# 2. Force UTF-8 encoding when reading the file
try:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        graph = json.load(f)
    print(f"[SUCCESS] Loaded {len(graph)} stations into memory.")
except FileNotFoundError:
    print(f"[ERROR] 'delhi_metro_graph.json' not found in {script_dir}.")
    print("Run the graph builder script first to generate it.")
    sys.exit()

# --- THE DMRC FARE CALCULATOR ---
def get_fare(distance_km):
    if distance_km <= 2.0: return 10
    elif distance_km <= 5.0: return 20
    elif distance_km <= 12.0: return 30
    elif distance_km <= 21.0: return 40
    elif distance_km <= 32.0: return 50
    else: return 60

# --- THE PATHFINDING ALGORITHM ---
def find_fastest_route(start_id, end_id):
    if start_id not in graph:
        print(f"[ERROR] Start ID '{start_id}' not found in the graph.")
        return None, 0, 0
    if end_id not in graph:
        print(f"[ERROR] End ID '{end_id}' not found in the graph.")
        return None, 0, 0

    pq = [(0, start_id, 0.0)]
    min_times = {start_id: 0}
    came_from = {}

    while pq:
        current_time, current_node, current_dist = heapq.heappop(pq)

        if current_node == end_id:
            path = []
            while current_node in came_from:
                path.append(current_node)
                current_node = came_from[current_node]
            path.append(start_id)
            path.reverse()
            return path, current_time, current_dist

        for edge in graph[current_node]["connections"]:
            neighbor = edge["to"]
            new_time = current_time + edge["time_sec"]
            new_dist = current_dist + edge["distance_km"]

            if neighbor not in min_times or new_time < min_times[neighbor]:
                min_times[neighbor] = new_time
                came_from[neighbor] = current_node
                heapq.heappush(pq, (new_time, neighbor, new_dist))

    return None, 0, 0

# --- TEST EXECUTIONS ---
START_STATION = "SHAHEED_STHAL(FIRST_STATION)_RED_LINE" 
END_STATION = "MOHAN_NAGAR_RED_LINE" 

print(f"\n[INFO] Calculating route: {START_STATION} -> {END_STATION}...")
route, total_time_sec, total_dist_km = find_fastest_route(START_STATION, END_STATION)

if route:
    mins = total_time_sec // 60
    fare = get_fare(total_dist_km)
    
    print("\n[SUCCESS] ROUTE FOUND!")
    print(f"Time: {mins} minutes")
    print(f"Distance: {total_dist_km:.2f} km")
    print(f"Fare: Rs. {fare}")
    
    print("\n[NAVIGATION] Turn-by-Turn:")
    for i, stn in enumerate(route):
        node_data = graph[stn]
        if i == 0:
            print(f"   [BOARD] {node_data['name']} ({node_data['line']})")
        elif i == len(route) - 1:
            print(f"   [ARRIVE] {node_data['name']} ({node_data['line']})")
        else:
            prev_stn = route[i-1]
            if graph[prev_stn]['name'] == node_data['name'] and graph[prev_stn]['line'] != node_data['line']:
                print(f"   [TRANSFER] Walk to {node_data['line']} platform")
            else:
                print(f"   | Pass {node_data['name']}")
else:
    print("\n[FAILED] Route failed. Please check the Station IDs.")