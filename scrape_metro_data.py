import json
import time
import requests
import re
from bs4 import BeautifulSoup
import os
import sys

# --- 1. WINDOWS SAFETY & SETUP ---
if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')

script_dir = os.path.dirname(os.path.abspath(__file__))
input_graph_path = os.path.join(script_dir, 'delhi_metro_graph.json')
output_graph_path = os.path.join(script_dir, 'delhi_metro_graph_real.json')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# --- 2. HELPER FUNCTIONS ---
def format_slug(station_name):
    """Converts 'Shaheed Sthal(First Station)' to 'shaheed-sthal-delhi-metro-station'"""
    # Remove anything inside parentheses, strip whitespace, and convert to lowercase
    clean_name = re.sub(r'\(.*?\)', '', station_name).strip().lower()
    # Replace spaces with hyphens
    slug = clean_name.replace(" ", "-")
    return f"{slug}-delhi-metro-station"

def parse_scraped_text(raw_text):
    """Extracts Time in seconds from the raw website text."""
    # Look for TimeH:MM
    time_match = re.search(r'Time(\d+):(\d+)', raw_text)
    
    if time_match:
        hours = int(time_match.group(1))
        minutes = int(time_match.group(2))
        total_time_sec = (hours * 3600) + (minutes * 60)
        return total_time_sec
    return None

# --- 3. LOAD THE GRAPH ---
print("[INFO] Loading Metro Graph...")
try:
    with open(input_graph_path, 'r', encoding='utf-8') as f:
        graph = json.load(f)
    print(f"[SUCCESS] Loaded {len(graph)} stations.")
except FileNotFoundError:
    print(f"[ERROR] '{input_graph_path}' not found. Run the graph builder first.")
    sys.exit()

# Tracking dictionary to avoid scraping the same track in both directions
scraped_pairs = {}
api_calls = 0

print("\n[INFO] Starting the Smart Edge Scraper...")

# --- 4. THE SCRAPING ENGINE ---
for station_id, data in graph.items():
    station_a_name = data['name']
    
    for edge in data['connections']:
        # We only scrape actual rail connections, not walking transfers
        if edge['type'] == 'transfer':
            continue
            
        neighbor_id = edge['to']
        station_b_name = graph[neighbor_id]['name']
        
        # Create an alphabetical key so A->B and B->A are treated as the same edge
        pair_key = tuple(sorted([station_a_name, station_b_name]))
        
        if pair_key not in scraped_pairs:
            slug_a = format_slug(station_a_name)
            slug_b = format_slug(station_b_name)
            url = f"https://delhimetrorail.info/{slug_a}-to-{slug_b}"
            
            print(f"[FETCHING] {station_a_name} -> {station_b_name}")
            
            try:
                response = requests.get(url, headers=HEADERS)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Extract all text from the page and strip blank lines
                    page_text = soup.get_text(separator=" ", strip=True)
                    
                    # Parse the text using our Regex logic
                    real_time_sec = parse_scraped_text(page_text)
                    
                    if real_time_sec:
                        scraped_pairs[pair_key] = real_time_sec
                        print(f"   -> Success: {real_time_sec // 60} minutes")
                    else:
                        print(f"   -> [WARNING] Could not parse time from page text.")
                else:
                    print(f"   -> [WARNING] URL returned status code {response.status_code}")
                
                api_calls += 1
                
                # CRITICAL: Pause to prevent DDOS flags
                time.sleep(1.5)
                
            except requests.exceptions.RequestException as e:
                print(f"   -> [ERROR] Network Error: {e}")
                time.sleep(5) # Wait longer if we hit a network block

# --- 5. UPDATE GRAPH WITH REALITY ---
print(f"\n[INFO] Scraping complete. Total requests made: {api_calls}")
print("[INFO] Injecting real times into the Graph...")

updated_edges = 0
for station_id, data in graph.items():
    station_a_name = data['name']
    for edge in data['connections']:
        if edge['type'] == 'rail':
            station_b_name = graph[edge['to']]['name']
            pair_key = tuple(sorted([station_a_name, station_b_name]))
            
            if pair_key in scraped_pairs:
                edge['time_sec'] = scraped_pairs[pair_key]
                updated_edges += 1

# Save the calibrated graph
with open(output_graph_path, 'w', encoding='utf-8') as f:
    json.dump(graph, f, indent=4)

print(f"[SUCCESS] Updated {updated_edges} directional edges.")
print(f"[SUCCESS] Calibrated graph saved to '{output_graph_path}'")