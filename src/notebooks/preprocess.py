import marimo

__generated_with = "0.17.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb

    from pathlib import Path
    return Path, duckdb


@app.cell
def _(Path, duckdb):
    data_src_path = Path("/home/saladass/crafts/gnn-ids/data/original")
    duck_conn = duckdb.connect()
    return (duck_conn,)


@app.cell
def _(duck_conn):
    duck_conn.sql("drop table if exists nb15_desc")
    duck_conn.read_csv("data/original/NUSW-NB15_features.csv").to_table("nb15_desc")
    return


@app.cell
def _(duck_conn):
    duck_conn.sql("""
        select distinct type
        from nb15_desc
    """)
    return


@app.cell
def _(duck_conn):
    duck_conn.sql("drop table if exists nb15_types")
    duck_conn.sql(
        """
        select 
            lower(name), 
            case
                when lower(name) != 'ct_ftp_cmd' and lower(type) in ['binary', 'Binary', 'integer', 'Integer'] then 'BIGINT'
                when lower(type) = 'float' then 'DOUBLE'
                when lower(type) = 'timestamp' then 'TIMESTAMP'
                else 'VARCHAR'
            end as type
        from nb15_desc
        """
    ).to_table("nb15_types")
    return


@app.cell
def _(duck_conn):
    duck_conn.table("nb15_types").to_df()
    return


@app.cell
def _():
    csv_files = [
        "data/original/UNSW-NB15_1.csv",
        "data/original/UNSW-NB15_2.csv",
        "data/original/UNSW-NB15_3.csv",
        "data/original/UNSW-NB15_4.csv",
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
    return columns, csv_files


@app.cell
def _(columns, csv_files, duck_conn):
    def process_unsw_nb15_data(duck_conn, columns):
        duck_conn.sql("create or replace sequence serial")
        duck_conn.sql("drop table if exists nb15_cleaned")
        duck_conn.read_csv(csv_files, header=False, columns=columns).select(r"""
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
        """).select(r"""
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
        """).select(r"""
            nextval('serial') as id,
            *
        """).to_table("nb15_cleaned")


    process_unsw_nb15_data(duck_conn, columns)
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"""
        from nb15_cleaned
        limit 10
    """).df()
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"""
        select * from nb15_train_featurized  
    """).fetch_df_chunk()
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"""
        copy nb15_train_featurized to 'nb15_train.parquet'
        (format parquet)
    """ )


    duck_conn.sql(r"""
        copy nb15_val_featurized to 'nb15_val.parquet'
        (format parquet)
    """ )


    duck_conn.sql(r"""
        copy nb15_test_featurized to 'nb15_test.parquet'
        (format parquet)
    """ )
    return


if __name__ == "__main__":
    app.run()
