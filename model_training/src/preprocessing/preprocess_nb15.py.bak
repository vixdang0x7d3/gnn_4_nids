import os.path as osp
import argparse
import duckdb

csvs = [
    "UNSW-NB15_1.csv",
    "UNSW-NB15_2.csv",
    "UNSW-NB15_3.csv",
    "UNSW-NB15_4.csv",
]

columns = {
    "srcip": "VARCHAR",
    "sport": "VARCHAR",
    "dstip": "VARCHAR",
    "dsport": "VARCHAR",
    "proto": "VARCHAR",
    "state": "VARCHAR",
    "dur": "DOUBLE",
    "sbytes": "BIGINT",
    "dbytes": "BIGINT",
    "sttl": "BIGINT",
    "dttl": "BIGINT",
    "sloss": "BIGINT",
    "dloss": "BIGINT",
    "service": "VARCHAR",
    "sload": "DOUBLE",
    "dload": "DOUBLE",
    "spkts": "BIGINT",
    "dpkts": "BIGINT",
    "swin": "BIGINT",
    "dwin": "BIGINT",
    "stcpb": "BIGINT",
    "dtcpb": "BIGINT",
    "smeansz": "BIGINT",
    "dmeansz": "BIGINT",
    "trans_depth": "BIGINT",
    "res_bdy_len": "BIGINT",
    "sjit": "DOUBLE",
    "djit": "DOUBLE",
    "stime": "BIGINT",
    "ltime": "BIGINT",
    "sintpkt": "DOUBLE",
    "dintpkt": "DOUBLE",
    "tcprtt": "DOUBLE",
    "synack": "DOUBLE",
    "ackdat": "DOUBLE",
    "is_sm_ips_ports": "BIGINT",
    "ct_state_ttl": "BIGINT",
    "ct_flw_http_mthd": "BIGINT",
    "is_ftp_login": "BIGINT",
    "ct_ftp_cmd": "VARCHAR",
    "ct_srv_src": "BIGINT",
    "ct_srv_dst": "BIGINT",
    "ct_dst_ltm": "BIGINT",
    "ct_src_ltm": "BIGINT",
    "ct_src_dport_ltm": "BIGINT",
    "ct_dst_sport_ltm": "BIGINT",
    "ct_dst_src_ltm": "BIGINT",
    "attack_cat": "VARCHAR",
    "label": "BIGINT",
}


def process(conn: duckdb.DuckDBPyConnection, raw_path: str) -> None:
    csv_paths = [osp.join(raw_path, csv) for csv in csvs]

    # create serial ID column
    conn.sql("create or replace sequence serial")

    (
        conn.read_csv(csv_paths, header=False, columns=columns)  # ty: ignore
        .select(r"""
            * replace(

                case
                    when sport = '-' then -1
                    else cast(sport AS BIGINT)
                end as sport,

                case
                    when dsport = '-' then -1
                    else cast(dsport AS BIGINT)
                end as dsport,

                to_timestamp(stime) as stime,

                to_timestamp(ltime) as ltime,

                coalesce(is_ftp_login, 0) as is_ftp_login,

                coalesce(ct_flw_http_mthd, 0) as ct_flw_http_mthd,

                case
                    when lower(proto) in ['udp', 'tcp'] then lower(proto)
                    else 'other'
                end as proto,

                case
                    when lower(state) in ['fin', 'con', 'int'] then lower(state)
                    else 'other'
                end as state,

                case
                    when service = '-' then 'unknown'
                    when lower(service) = 'dns' then 'dns'
                    else 'other'
                end as service,

                cast(replace(ct_ftp_cmd, ' ', '0') as BIGINT) as ct_ftp_cmd,


                case
                    when trim(attack_cat) == 'Backdoor' then 'Backdoors'
                    else coalesce(trim(attack_cat), 'Normal') 
                end as attack_cat
            )
        """)
        .select(r"""
            *, "multiclass_label": (case
            when attack_cat = 'Normal' then 0
            when attack_cat = 'Worms' then 1
            when attack_cat = 'Backdoors' then 2
            when attack_cat = 'Shellcode' then 3
            when attack_cat = 'Analysis' then 4
            when attack_cat = 'Reconnaissance' then 5
            when attack_cat = 'DoS' then 6
            when attack_cat = 'Fuzzers' then 7
            when attack_cat = 'Exploits' then 8
            when attack_cat = 'Generic' then 9 
            end)::bigint
        """)
        .select(r"nextval('serial') as id, *")
    ).to_table("processed")

    return


def save(
    conn: duckdb.DuckDBPyConnection,
    save_path: str,
    scale_factor: int = 100,
    split: bool = True,
) -> None:
    if split:
        conn.sql(r"drop table if exists trainval")
        conn.sql(r"""
            create table trainval as
            from processed
            using sample 80 percent (bernoulli, 42)
        """)

        conn.sql(r"drop table if exists test")
        conn.sql(r"""
            create table test as 
            from processed
            anti join trainval using (id)
        """)

        conn.sql(r"drop table if exists train")
        conn.sql(r"""
            create table train as
            from trainval
            using sample 75 percent (bernoulli, 42)
        """)

        conn.sql(r"drop table if exists val")
        conn.sql(r"""
            create table val as
            from trainval
            anti join train using (id)
        """)

        conn.sql(
            f"select * exclude id from train using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "train.parquet"))
        conn.sql(
            f"select * exclude id from val using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "val.parquet"))
        conn.sql(
            f"select * exclude id from test using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "test.parquet"))
    else:
        conn.sql(
            f"select * exclude id from processed using sample {scale_factor}%"
        ).write_parquet(osp.join(save_path, "processed.parquet"))

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dataset preprocessing script for UNSW-NB15 csv dataset",
        epilog="Example: python preprocess_nb15.py --raw_path ./raw --save_path ./data/nb15 --split --sf 0.1",
    )
    parser.add_argument(
        "--raw_path",
        help="Path to directory with UNSW-NB15 csv files",
        required=True,
    )
    parser.add_argument(
        "--save_path",
        help="Path where the parquets are saved",
        required=True,
    )
    parser.add_argument(
        "--split",
        help="Enable splitting",
        action="store_true",
    )
    parser.add_argument(
        "--sf",
        type=int,
        help="Scale factor for dataset subsampling",
    )

    args = parser.parse_args()

    if args.sf is None or args.sf > 1 or args.sf < 0:
        scale_factor = 100
    else:
        scale_factor = args.sf * 100

    duckconn = duckdb.connect()

    process(duckconn, args.raw_path)
    save(duckconn, args.save_path, scale_factor, args.split)
