import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb

    from pathlib import Path
    return Path, duckdb


@app.cell
def _(Path, duckdb):
    data_src_path = Path("/home/saladass/crafts/gnn-ids/dataset")

    duck_conn = duckdb.connect("dataset/nb15.duckdb")
    return (duck_conn,)


@app.cell
def _(duck_conn):
    duck_conn.sql("drop table if exists nb15_desc")
    duck_conn.read_csv("dataset/original/NUSW-NB15_features.csv").to_table("nb15_desc")
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
        "dataset/original/UNSW-NB15_1.csv",
        "dataset/original/UNSW-NB15_2.csv",
        "dataset/original/UNSW-NB15_3.csv",
        "dataset/original/UNSW-NB15_4.csv",
    ]

    columns = {
        "srcip": "VARCHAR",
        "sport": "BIGINT",
        "dstip": "VARCHAR",
        "dsport": "BIGINT",
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
                    when service = '-' then 'no_service'
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
           * exclude(srcip, sport, dstip, dsport, ltime, stime)
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
        select * 
        from nb15_cleaned
    """).df()
    return


@app.cell
def _(duck_conn):
    duck_conn.sql("""drop table if exists nb15_onehots""")

    duck_conn.sql(r"""
        with proto_onehot as (
            pivot nb15_cleaned
            on proto
            using coalesce(max(proto = proto)::int, 0) as proto_onehot
            group by proto
        ),
        service_onehot as (
            pivot nb15_cleaned
            on service
            using coalesce(max(service = service), 0) as service_onehot
            group by service
        ),
        state_onehot as (
            pivot nb15_cleaned
            on state
            using coalesce(max(state = state), 0) as state_onehot
            group by state
        )
        select
            nb15_cleaned.* exclude (proto, state, service),
            proto_onehot.* like '%\_proto_onehot' escape '\',
            state_onehot.* like '%\_state_onehot' escape '\',
            service_onehot.* like '%\_service_onehot' escape '\'
        from nb15_cleaned
        inner join proto_onehot using (proto)
        inner join state_onehot using (state)
        inner join service_onehot using (service);
    """ ).to_table("nb15_onehots")
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"""
        select columns('\w+_onehot') from nb15_onehots
    """).fetch_df_chunk()
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"set threads=1")

    duck_conn.sql("drop table if exists nb15_trainval")
    duck_conn.sql(r"""
        create table nb15_trainval as
        from nb15_onehots
        using sample 80 percent (bernoulli, 42)
    """)

    duck_conn.sql(r"set threads=8")

    duck_conn.sql("drop table if exists nb15_testset")
    duck_conn.sql(r"""
        create table nb15_testset as 
        from nb15_onehots
        anti join nb15_trainval using (id)
    """)

    duck_conn.sql(r"set threads=1")

    duck_conn.sql("drop table if exists nb15_trainset")
    duck_conn.sql(r"""
        create table nb15_trainset as
        from nb15_trainval
        using sample 75 percent (bernoulli, 42)
    """)

    duck_conn.sql(r"set threads=8")

    duck_conn.sql("drop table if exists nb15_valset")
    duck_conn.sql(r"""
        create table nb15_valset as 
        from nb15_trainval
        anti join nb15_trainset using (id)
    """)
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"""
        summarize nb15_trainset
    """).to_df()
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"""
    CREATE OR REPLACE MACRO scaling_params(table_name, column_list) AS TABLE
        FROM query_table(table_name)
        SELECT
            "avg_\0": avg(columns(column_list)),
            "std_\0": stddev_pop(columns(column_list)),
            "min_\0": min(columns(column_list)),
            "max_\0": max(columns(column_list));
    """)

    duck_conn.sql(r"""
    CREATE OR REPLACE MACRO min_max_scaler(val, min_val, max_val) AS
    (val - min_val) / nullif(max_val - min_val, 0);
    """)
    return


@app.cell
def _():
    to_standardize  = [
        'dur',
        'sbytes',
        'dbytes',
        'sttl',
        'dttl',
        'sloss',
        'dloss',
        'sload',
        'dload',
        'spkts',
        'dpkts',
        'swin',
        'dwin',
        'stcpb',
        'dtcpb',
        'smeansz',
        'dmeansz',
        'trans_depth',
        'res_bdy_len',
        'sjit',
        'djit',
        'sintpkt',
        'dintpkt',
        'tcprtt',
        'synack',
        'ackdat',
        'is_sm_ips_ports',
        'ct_state_ttl',
        'ct_flw_http_mthd',
        'is_ftp_login',
        'ct_ftp_cmd',
        'ct_srv_src',
        'ct_srv_dst',
        'ct_dst_ltm',
        'ct_src_ltm',
        'ct_src_dport_ltm',
        'ct_dst_sport_ltm',
        'ct_dst_src_ltm',
    ]

    # having dbt would be so great here :-)
    def build_standardize_query(transform_table, fit_table, candidates, exclude_cols=['id', 'attack_cat']):
        select_cols = [
            f"min_max_{f}: min_max_scaler({f}, min_{f}, max_{f})"
            for f in candidates
        ]

        return f"""
        select
            {fit_table}.* exclude ({', '.join(exclude_cols + candidates)}),
            {',\n        '.join(select_cols)}
        from {fit_table},
            scaling_params('{transform_table}', {candidates})
        """
    return build_standardize_query, to_standardize


@app.cell
def _(build_standardize_query, duck_conn, to_standardize):
    duck_conn.sql("drop table if exists nb15_train_featurized")
    duck_conn.sql(
        build_standardize_query('nb15_trainset', 'nb15_trainset', to_standardize)
    ).to_table('nb15_train_featurized')

    duck_conn.sql("drop table if exists nb15_val_featurized")
    duck_conn.sql(
        build_standardize_query('nb15_trainset', 'nb15_valset', to_standardize)
    ).to_table('nb15_val_featurized')

    duck_conn.sql("drop table if exists nb15_test_featurized")
    duck_conn.sql(
        build_standardize_query('nb15_trainset', 'nb15_testset', to_standardize)
    ).to_table('nb15_test_featurized')
    return


@app.cell
def _(duck_conn):
    duck_conn.sql(r"""
        select * from nb15_train_featurized  
    """).fetch_df_chunk()
    return


if __name__ == "__main__":
    app.run()
