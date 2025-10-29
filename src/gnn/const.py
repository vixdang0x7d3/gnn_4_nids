# remember to change this every time you modify the upstream data pipeline
GDRIVE_URL = "https://drive.google.com/file/d/1dTKTmNuhUm4ys5H9GXo_0i22joS73HWd/view?usp=drive_link"

BINARY_LABEL = "label"
MULTICLASS_LABEL = "multiclass_label"

FEATURE_ATTRIBUTES = [
    "min_max_sttl",
    "min_max_dur",
    "min_max_dintpkt",
    "min_max_sintpkt",
    "min_max_ct_dst_sport_ltm",
    "min_max_tcprtt",
    "min_max_sbytes",
    "min_max_dbytes",
    "min_max_smeansz",
    "min_max_dmeansz",
    "min_max_sload",
    "min_max_dload",
    "min_max_spkts",
    "min_max_dpkts",
]

EDGE_ATTRIBUTES = ["proto", "service", "state"]
