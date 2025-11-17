# remember to change this every time you modify the upstream data pipeline
GDRIVE_URL = "https://drive.google.com/file/d/1dTKTmNuhUm4ys5H9GXo_0i22joS73HWd/view?usp=drive_link"

BINARY_LABEL = "label"
MULTICLASS_LABEL = "multiclass_label"


TO_STANDARDIZE = [
    "dur",
    "sbytes",
    "dbytes",
    "sttl",
    "dttl",
    "sloss",
    "dloss",
    "sload",
    "dload",
    "spkts",
    "dpkts",
    "swin",
    "dwin",
    "stcpb",
    "dtcpb",
    "smeansz",
    "dmeansz",
    "trans_depth",
    "res_bdy_len",
    "sjit",
    "djit",
    "sintpkt",
    "dintpkt",
    "tcprtt",
    "synack",
    "ackdat",
    "is_sm_ips_ports",
    "ct_state_ttl",
    "ct_flw_http_mthd",
    "is_ftp_login",
    "ct_ftp_cmd",
    "ct_srv_src",
    "ct_srv_dst",
    "ct_dst_ltm",
    "ct_src_ltm",
    "ct_src_dport_ltm",
    "ct_dst_sport_ltm",
    "ct_dst_src_ltm",
]


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

EDGE_ATTRIBUTES = ["proto", "service", "state"]
