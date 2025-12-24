#!/usr/bin/env uv run

# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb",
# ]
# ///

import os
import os.path as osp
import argparse
import duckdb

# --- CẤU HÌNH SCHEMA CHÍNH XÁC ---
columns = {
    "FLOW_START_MILLISECONDS": "BIGINT",
    "FLOW_END_MILLISECONDS": "BIGINT",
    "IPV4_SRC_ADDR": "VARCHAR",
    "L4_SRC_PORT": "BIGINT",
    "IPV4_DST_ADDR": "VARCHAR",
    "L4_DST_PORT": "BIGINT",
    "PROTOCOL": "BIGINT",
    "L7_PROTO": "DOUBLE",
    "IN_BYTES": "BIGINT",
    "IN_PKTS": "BIGINT",
    "OUT_BYTES": "BIGINT",
    "OUT_PKTS": "BIGINT",
    "TCP_FLAGS": "BIGINT",
    "CLIENT_TCP_FLAGS": "BIGINT",
    "SERVER_TCP_FLAGS": "BIGINT",
    "FLOW_DURATION_MILLISECONDS": "BIGINT",
    "DURATION_IN": "BIGINT",
    "DURATION_OUT": "BIGINT",
    "MIN_TTL": "BIGINT",
    "MAX_TTL": "BIGINT",
    "LONGEST_FLOW_PKT": "BIGINT",
    "SHORTEST_FLOW_PKT": "BIGINT",
    "MIN_IP_PKT_LEN": "BIGINT",
    "MAX_IP_PKT_LEN": "BIGINT",
    "SRC_TO_DST_SECOND_BYTES": "DOUBLE",
    "DST_TO_SRC_SECOND_BYTES": "DOUBLE",
    "RETRANSMITTED_IN_BYTES": "BIGINT",
    "RETRANSMITTED_IN_PKTS": "BIGINT",
    "RETRANSMITTED_OUT_BYTES": "BIGINT",
    "RETRANSMITTED_OUT_PKTS": "BIGINT",
    "SRC_TO_DST_AVG_THROUGHPUT": "BIGINT",
    "DST_TO_SRC_AVG_THROUGHPUT": "BIGINT",
    "NUM_PKTS_UP_TO_128_BYTES": "BIGINT",
    "NUM_PKTS_128_TO_256_BYTES": "BIGINT",
    "NUM_PKTS_256_TO_512_BYTES": "BIGINT",
    "NUM_PKTS_512_TO_1024_BYTES": "BIGINT",
    "NUM_PKTS_1024_TO_1514_BYTES": "BIGINT",
    "TCP_WIN_MAX_IN": "BIGINT",
    "TCP_WIN_MAX_OUT": "BIGINT",
    "ICMP_TYPE": "BIGINT",
    "ICMP_IPV4_TYPE": "BIGINT",
    "DNS_QUERY_ID": "BIGINT",
    "DNS_QUERY_TYPE": "BIGINT",
    "DNS_TTL_ANSWER": "BIGINT",
    "FTP_COMMAND_RET_CODE": "BIGINT",
    "SRC_TO_DST_IAT_MIN": "BIGINT",
    "SRC_TO_DST_IAT_MAX": "BIGINT",
    "SRC_TO_DST_IAT_AVG": "BIGINT",
    "SRC_TO_DST_IAT_STDDEV": "BIGINT",
    "DST_TO_SRC_IAT_MIN": "BIGINT",
    "DST_TO_SRC_IAT_MAX": "BIGINT",
    "DST_TO_SRC_IAT_AVG": "BIGINT",
    "DST_TO_SRC_IAT_STDDEV": "BIGINT",
    "Label": "BIGINT",
    "Attack": "VARCHAR",
}


def process(conn: duckdb.DuckDBPyConnection, raw_path: str) -> None:
    # Tên file CSV đầu vào
    csv_path = osp.join(raw_path, "NF-UNSW-NB15-v3.csv")
    conn.sql("create or replace sequence serial")

    # Đọc file và xử lý
    (
        conn.read_csv(csv_path, header=True, columns=columns)
        .select(r"""
            *,
            -- BƯỚC 1: Clean Label
            case
                when Label = 0 then 'Normal'
                when lower(trim(Attack)) = 'benign' then 'Normal'
                when trim(Attack) = 'Backdoor' then 'Backdoors'
                else coalesce(trim(Attack), 'Normal') 
            end as attack_cat_clean
        """)
        .select(r"""
            *,
            -- BƯỚC 2: Map sang số (Multiclass)
            (case
                when attack_cat_clean = 'Normal' then 0
                when attack_cat_clean = 'Worms' then 1
                when attack_cat_clean = 'Backdoors' then 2
                when attack_cat_clean = 'Shellcode' then 3
                when attack_cat_clean = 'Analysis' then 4
                when attack_cat_clean = 'Reconnaissance' then 5
                when attack_cat_clean = 'DoS' then 6
                when attack_cat_clean = 'Fuzzers' then 7
                when attack_cat_clean = 'Exploits' then 8
                when attack_cat_clean = 'Generic' then 9 
                else 9 
            end)::bigint as multiclass_label,

            -- Tạo ID để split
            nextval('serial') as id
        """)
    ).to_table("processed")

    return


def save(
    conn: duckdb.DuckDBPyConnection,
    save_path: str,
    scale_factor: int = 100,
    split: bool = True,
) -> None:
    if split:
        print(" -> Splitting data into Train/Val/Test...")
        conn.sql("drop table if exists trainval")
        conn.sql("""
            create table trainval as
            from processed
            using sample 80 percent (bernoulli, 42)
        """)

        conn.sql("drop table if exists test")
        conn.sql("""
            create table test as 
            from processed
            anti join trainval using (id)
        """)

        conn.sql("drop table if exists train")
        conn.sql("""
            create table train as
            from trainval
            using sample 75 percent (bernoulli, 42)
        """)

        conn.sql("drop table if exists val")
        conn.sql("""
            create table val as
            from trainval
            anti join train using (id)
        """)

        print(
            f" -> Saving parquets to {save_path} with scale factor {scale_factor}%..."
        )

        # CHỈ LOẠI BỎ CỘT ID
        conn.sql(
            f"select * exclude (id) from train using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "train.parquet"))

        conn.sql(
            f"select * exclude (id) from val using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "val.parquet"))

        conn.sql(
            f"select * exclude (id) from test using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "test.parquet"))
    else:
        print(f" -> Saving full processed parquet to {save_path}...")
        conn.sql(
            f"select * exclude (id) from processed using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "processed.parquet"))

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dataset preprocessing script for NF-UNSW-NB15-v3 dataset",
    )
    parser.add_argument("--raw_path", required=True)
    parser.add_argument("--save_path", required=True)
    parser.add_argument("--split", action="store_true")
    parser.add_argument("--sf", type=int, default=1)

    args = parser.parse_args()

    scale_factor = args.sf * 100
    if scale_factor > 100:
        scale_factor = 100
    if scale_factor < 0:
        scale_factor = 100

    if not osp.exists(args.save_path):
        os.makedirs(args.save_path)

    duckconn = duckdb.connect()

    process(duckconn, args.raw_path)
    save(duckconn, args.save_path, scale_factor, args.split)
    print("Done.")
