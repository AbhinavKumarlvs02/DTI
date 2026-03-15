import pandas as pd
import json

# --- CONFIGURATION ---
FILE_PATH = "Delhi_metro.csv"
OUTPUT_FILE = "delhi_metro_graph.json"
AVG_SPEED_KMPH = 35.0  # Used to calculate travel time between stations

print(f"--- LOADING DATA FROM {FILE_PATH} ---")

try:
    df = pd.read_csv(FILE_PATH)
except FileNotFoundError:
    print(f"Error: '{FILE_PATH}' not found. Please check the file name and location.")
    exit()

# Clean column names to remove any accidental leading/trailing spaces in the CSV
df.columns = df.columns.str.strip()

# Map exact column names from your dataset
COL_NAME = "Station Names"
COL_DIST = "Dist. From First Station(km)"
COL_LINE = "Metro Line"
COL_LAT = "Latitude"
COL_LON = "Longitude"

metro_graph = {}

print("--- BUILDING METRO GRAPH ---")

# 1. ADD ALL NODES
for _, row in df.iterrows():
    # Clean the strings
    station_name = str(row[COL_NAME]).strip()
    line_name = str(row[COL_LINE]).strip()
    
    # Create a unique, strict ID: e.g., "MOHAN_NAGAR_RED_LINE"
    station_id = f"{station_name}_{line_name}".replace(" ", "_").upper()
    
    metro_graph[station_id] = {
        "name": station_name,
        "line": line_name,
        "lat": float(row[COL_LAT]),
        "lon": float(row[COL_LON]),
        "connections": [] # We will fill this next
    }

# 2. CREATE INTRA-LINE EDGES (Connect stations along the same track)
for line_name, group in df.groupby(COL_LINE):
    # Sort strictly by distance to ensure the physical sequence is correct
    group = group.sort_values(by=COL_DIST)
    stations = group.to_dict('records')
    
    for i in range(len(stations) - 1):
        current_stn = stations[i]
        next_stn = stations[i+1]
        
        curr_name = str(current_stn[COL_NAME]).strip()
        next_name = str(next_stn[COL_NAME]).strip()
        
        curr_id = f"{curr_name}_{line_name}".replace(" ", "_").upper()
        next_id = f"{next_name}_{line_name}".replace(" ", "_").upper()
        
        # Calculate precise distance and time
        dist_curr = float(current_stn[COL_DIST])
        dist_next = float(next_stn[COL_DIST])
        edge_dist_km = round(abs(dist_next - dist_curr), 2)
        
        # Time = (Distance / Speed) converted to seconds
        travel_time_sec = int((edge_dist_km / AVG_SPEED_KMPH) * 3600)
        
        # Add connection (Forward direction)
        metro_graph[curr_id]["connections"].append({
            "to": next_id,
            "distance_km": edge_dist_km,
            "time_sec": travel_time_sec,
            "type": "rail"
        })
        
        # Add connection (Backward direction)
        metro_graph[next_id]["connections"].append({
            "to": curr_id,
            "distance_km": edge_dist_km,
            "time_sec": travel_time_sec,
            "type": "rail"
        })

# 3. CREATE INTERCHANGE TRANSFER EDGES (The Walking Penalty)
# Find stations with the exact same name but on different lines
for name, group in df.groupby(COL_NAME):
    if len(group) > 1:
        interchange_stations = group.to_dict('records')
        
        # Connect every platform to every other platform at this interchange
        for i in range(len(interchange_stations)):
            for j in range(len(interchange_stations)):
                if i != j:
                    stn_a = interchange_stations[i]
                    stn_b = interchange_stations[j]
                    
                    line_a = str(stn_a[COL_LINE]).strip()
                    line_b = str(stn_b[COL_LINE]).strip()
                    
                    id_a = f"{str(stn_a[COL_NAME]).strip()}_{line_a}".replace(" ", "_").upper()
                    id_b = f"{str(stn_b[COL_NAME]).strip()}_{line_b}".replace(" ", "_").upper()
                    
                    # Add walking transfer (Penalty: 5 minutes / 300 seconds)
                    metro_graph[id_a]["connections"].append({
                        "to": id_b,
                        "distance_km": 0.0,
                        "time_sec": 300, 
                        "type": "transfer"
                    })

print(f"Metro Graph Built! Total Nodes: {len(metro_graph)}")

# 4. EXPORT FOR THE OPTIMIZER
with open(OUTPUT_FILE, 'w') as f:
    json.dump(metro_graph, f, indent=4)

print(f"Success: Data saved securely to '{OUTPUT_FILE}'")