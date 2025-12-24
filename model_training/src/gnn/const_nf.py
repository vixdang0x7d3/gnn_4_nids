# NetFlow-based UNSW-NB15 feature constants

BINARY_LABEL = "Label"
MULTICLASS_LABEL = "multiclass_label"

# NetFlow features selected from nf-UNSW-NB15 dataset
# These are the key features from NetFlow v9/IPFIX format
FEATURE_ATTRIBUTES = [
    'MIN_TTL',                      # Minimum TTL value
    'FLOW_DURATION_MILLISECONDS',   # Flow duration in milliseconds
    'TCP_WIN_MAX_OUT',              # Maximum TCP window size (outbound)
    'SERVER_TCP_FLAGS',             # TCP flags from server
    'DST_TO_SRC_IAT_MAX',          # Maximum inter-arrival time (dst->src)
    'SRC_TO_DST_IAT_MAX',          # Maximum inter-arrival time (src->dst)
    'IN_BYTES',                     # Inbound bytes
    'OUT_BYTES',                    # Outbound bytes
    'NUM_PKTS_UP_TO_128_BYTES',    # Number of packets up to 128 bytes
    'DST_TO_SRC_AVG_THROUGHPUT',   # Average throughput (dst->src)
    'SRC_TO_DST_SECOND_BYTES',     # Second packet bytes (src->dst)
    'DNS_QUERY_TYPE',               # DNS query type (0 if not DNS)
    'FTP_COMMAND_RET_CODE'          # FTP command return code (0 if not FTP)
]

# Edge attributes for graph construction
# Nodes are connected if they share the same (PROTOCOL, L7_PROTO) pair
EDGE_ATTRIBUTES = [
    'PROTOCOL',     # IP protocol number (6=TCP, 17=UDP, etc.)
    'L7_PROTO'      # Layer 7 protocol (from nDPI)
]
