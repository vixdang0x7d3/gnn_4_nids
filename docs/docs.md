# GNN4NIDS Documentation

This directory contains project documentation, change log, and architectural design for the codebase.

## table of content

- [Requirements](#requirements)
- [Overview](#overview)
- [UNSW-NB15 Dataset](#dataset)
- [Data pipeline](#data-pipeline)
- [PyG and NetworkX](#pyg-networkx)
- [Building Graph Neural Network](#gnn-model)
- [Ingesting Zeek logs](#zeek-ingestion)
- [Change log](changelog.md)

## overview

Network Intrusion Detection System (NIDS) is a system that attempts to detect hacking activities e.g. Denial of Service, Port scanning, on a computer network or a computer itself. The NIDS monitors network traffic and helps to detect these malicious activities by identifying suspicious patterns in the incoming packets.

This project aims to leverages a Geometric Deep Learning approach, in particular Graph Neural Networks (GNN) to model data transfer in network systems and ultimately exploits the benefits of using such models in network intrusion detection.

The model training and experimentation process are carried out on the UNSW-NB15 dataset.

## requirements

### expected outcome (business perspective)
- Alert end-user when there is anomaly.
- Can handle real-time packet streams.
- Data is transformed into a format that the end-user can understand.
- Visualize what, when are the detected anomalies happened.
- Visualize statistics of attacks in a specific interval .

### tasks (engineering perspetive)

- Exploration of the UNSW-NB15 dataset
- Perform data preprocessing and graph construction
- Train & evaluate GCN model
- Build a reproducible data pipeline
- Integrating with SIEM/alert system

## dataset

The UNSW-NB15 dataset was developed at the University of New South Wales (UNSW). It aims to to provide a modern network benchmark dataset. It improves upon existing datasets like KDD98 by including newer attacks, which are realized using the IXIA PerfectStorm tool, providing both normal and malicious behavior. Data is mostly intended for anomaly-related use-cases by leveraging features derived from network traffic.

Both benign and malicious activity is managed and executed by IXIA Perfect Storm tool, specifically its traffic generator. However, details regarding either process are not given. Attacks are grouped into 9 categories:

- Fuzzers
- Analysis
- Backdoors
- DoS
- Exploits
- Generic
- Reconnaissance
- Shellcode
- Worms

Additionally, IXIA supplies a ground truth file, listing information like start time or source port and address. Two simulation runs were performed, which differ only in the frequency of their attacks. During the first run (16 hours), one attack is executed per second. During the second run (15 hours), that number increases to ten attacks per second.

Pcaps of presumably the entire network are collected at a central location (Router 1) using tcpdump. From these files, 49 features are derived, which are grouped into: 

- 5 flow features (e.g. Source IP address)
- 13 basic features (e.g. Source to destination bytes)
- 8 content features (e.g. Source TCP sequence number)
- 9 time features (e.g. record start time)
- 12 additional features

Below is description table for each feature of the dataset:

| Name | Description |
|------|-------------|
| srcip | Source IP address |
| sport | Source port number |
| dstip | Destination IP address |
| dsport | Destination port number |
| proto | Transaction protocol |
| state | Indicates to the state and its dependent protocol, e.g. ACC, CLO, CON, ECO, ECR, FIN, INT, MAS, PAR, REQ, RST, TST, TXD, URH, URN, and (-) (if not used state) |
| dur | Record total duration |
| sbytes | Source to destination transaction bytes |
| dbytes | Destination to source transaction bytes |
| sttl | Source to destination time to live value |
| dttl | Destination to source time to live value |
| sloss | Source packets retransmitted or dropped |
| dloss | Destination packets retransmitted or dropped |
| service | http, ftp, smtp, ssh, dns, ftp-data, irc and (-) if not much used service |
| sload | Source bits per second |
| dload | Destination bits per second |
| spkts | Source to destination packet count |
| dpkts | Destination to source packet count |
| swin | Source TCP window advertisement value |
| dwin | Destination TCP window advertisement value |
| stcpb | Source TCP base sequence number |
| dtcpb | Destination TCP base sequence number |
| smeansz | Mean of the flow packet size transmitted by the src |
| dmeansz | Mean of the flow packet size transmitted by the dst |
| trans_depth | Represents the pipelined depth into the connection of http request/response transaction |
| res_bdy_len | Actual uncompressed content size of the data transferred from the servers http service |
| sjit | Source jitter (mSec) |
| djit | Destination jitter (mSec) |
| stime | Record start time |
| ltime | Record last time |
| sintpkt | Source interpacket arrival time (mSec) |
| dintpkt | Destination interpacket arrival time (mSec) |
| tcprtt | TCP connection setup round-trip time, the sum of synack and ackdat |
| synack | TCP connection setup time, the time between the SYN and the SYN_ACK packets |
| ackdat | TCP connection setup time, the time between the SYN_ACK and the ACK packets |
| is_sm_ips_ports | If source (1) and destination (3) IP addresses equal and port numbers (2)(4) equal then, this variable takes value 1 else 0 |
| ct_state_ttl | No. for each state (6) according to specific range of values for source/destination time to live (10) (11) |
| ct_flw_http_mthd | No. of flows that has methods such as Get and Post in http service |
| is_ftp_login | If the ftp session is accessed by user and password then 1 else 0 |
| ct_ftp_cmd | No of flows that has a command in ftp session |
| ct_srv_src | No. of connections that contain the same service (14) and source address (1) in 100 connections according to the last time (26) |
| ct_srv_dst | No. of connections that contain the same service (14) and destination address (3) in 100 connections according to the last time (26) |
| ct_dst_ltm | No. of connections of the same destination address (3) in 100 connections according to the last time (26) |
| ct_src_ltm | No. of connections of the same source address (1) in 100 connections according to the last time (26) |
| ct_src_dport_ltm | No of connections of the same source address (1) and the destination port (4) in 100 connections according to the last time (26) |
| ct_dst_sport_ltm | No of connections of the same destination address (3) and the source port (2) in 100 connections according to the last time (26) |
| ct_dst_src_ltm | No of connections of the same source (1) and the destination (3) address in 100 connections according to the last time (26) |
| attack_cat | The name of each attack category. In this data set, nine categories e.g. Fuzzers, Analysis, Backdoors, DoS, Exploits, Generic, Reconnaissance, Shellcode and Worms |
| label | 0 for normal and 1 for attack records |

For our model, we use the `UNSW-NB15_[1-4].csv` files. Since 

## data-pipelines

There are two separate data sources we need work with:

- The UNSW-NB15 dataset in the form of `.csv` files for training & testing.
- The Zeek logs (tab seperated csv format, different schema) for real-time inference.

Focus on the dataset first, then gradually figure out how to process Zeek logs.

### preprocessing for the UNSW-NB15 dataset (csv files)

The preprocessing process consists of a few steps:

Step 1:
- Load csv file into DuckDB
- Clean and standardize (string values, type conversion, null data, etc.)
- Drop sensitive columns (srcip, sport, dstip, dsport, stime, ltime)
- Normalize categorical columns (low frequency -> 'other')
- Create multi-label labels (e.g. Normal -> 0, Malware -> 1, ...)

Step 2:
- Perform onehot encoding on the categorical columns (proto, state, service)
- Drop the original columns

Step 3:
- Perform data split: 
    60% training (1.52m)
    20% validation (500k)
    20% testing (500k)


Step 4:
- Perform Min-max scaling on numerical columns (yeah all of them)
- Drop the originals

Note: Perform feature scaling after data split to avoid data leakage.

Featurized dataset contains 56 columns (after one-hot encoding). The whole dataset is saved as a duckdb file. Each featurized data split is saved as a parquet file.

### zeek logs data pipeline.
(TODO)

## pyg-networkx

## gnn-model

## zeek-ingestion
