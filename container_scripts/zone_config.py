# zone_config.py

# Simulated RTT (ms) between zones
ZONE_RTT = {
    ("FRA", "FRA"): 10,
    ("FRA", "IAD"): 90,
    ("FRA", "BOM"): 200,
    ("IAD", "FRA"): 90,
    ("IAD", "IAD"): 10,
    ("IAD", "BOM"): 250,
    ("BOM", "FRA"): 200,
    ("BOM", "IAD"): 250,
    ("BOM", "BOM"): 10,
}
