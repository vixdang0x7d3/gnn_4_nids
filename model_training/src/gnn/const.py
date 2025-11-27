BINARY_LABEL = "label"
MULTICLASS_LABEL = "multiclass_label"

# selected features by ../notebooks/feature_selection.py
FEATURE_ATTRIBUTES = [
    "sttl",
    "dur",
    "dintpkt",
    "sintpkt",
    "ct_dst_sport_ltm",
    "tcprtt",
    "sbytes",
    "dbytes",
    "smeansz",
    "dmeansz",
    "sload",
    "dload",
    "spkts",
    "dpkts",
]

# between two nodes form an edge if
# they have the same (proto, state, service) tripplet
EDGE_ATTRIBUTES = ["proto", "service", "state"]
