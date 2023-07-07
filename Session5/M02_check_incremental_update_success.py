# incremental update가 제대로 처리되었는지 확인하기 위한 test code
import os
import datetime
import psycopg2
import yfinance as yf

# constants
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST")
REDSHIFT_USER = os.environ.get("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")


# connect to redshift schema
def get_Redshift_connection(autocommit):
    host = REDSHIFT_HOST
    user = REDSHIFT_USER
    password = REDSHIFT_PASSWORD
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(
        f"dbname={dbname} user={user} host={host} password={password} port={port}"
    )
    conn.set_session(autocommit=autocommit)
    print(f"Redsift Connection Opened:")
    return conn


def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime("%Y-%m-%d %H:%M:%S")
        records.append(
            [date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]]
        )

    return records


def get_data_from_redshift(sql):
    conn = get_Redshift_connection(True)
    cur = conn.cursor()

    try:
        cur.execute(sql)
        data = cur.fetchall()
        print(f"Total data count : {len(data)}")
        print("Total data :")
        for d in data:
            print(d)

    except psycopg2.Error as e:
        print(f"Error occurred: {e}")

    finally:
        cur.close()
        conn.close()


# create dummy table in redshift schema
def create_dummy_table(conn, schema, table):
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(
            f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        date date,
        "open" float,
        high float,
        low float,
        close float,
        volume bigint,
        created_date timestamp default sysdate     
    );"""
        )
        cur.execute("COMMIT;")

    except psycopg2.Error as e:
        print(f"Error occurred: {e}")
        cur.execute("ROLLBACK;")

    finally:
        cur.close()
        conn.close()


# drop table from redshift schema
def drop_table_from_redshift(schema, table):
    conn = get_Redshift_connection(True)
    cur = conn.cursor()

    try:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        print(f"Table {schema}.{table} dropped.")

    except psycopg2.Error as e:
        print(f"Error occurred: {e}")

    finally:
        cur.close()
        conn.close()


# insert data into redshift schema
def insert_dummy_data_into_redshift(schema, table, records, custom_date):
    conn = get_Redshift_connection(True)
    cur = conn.cursor()

    try:
        custom_date = custom_date.strftime("%Y-%m-%d %H:%M:%S")
        row_count = 0
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]}, '{custom_date}');"
            print(sql)
            cur.execute(sql)
            row_count += 1
        print(f"{row_count} rows inserted into {schema}.{table}.")

    except psycopg2.Error as e:
        print(f"Error occurred: {e}")

    finally:
        cur.close()
        conn.close()


# copy data to temp table in redshift schema & insert data into redshift schema
def copy_dummy_data_into_redshift(schema, source_table, new_table):
    conn = get_Redshift_connection(True)
    cur = conn.cursor()
    query = f"""
    insert into {schema}.{new_table}
    select 
        date,
        "open",
        high,
        low,
        close,
        volume,
        created_date
    from
    (select 
        *, 
        row_number() over (partition by date order by created_date desc) seq
    from 
    {schema}.{source_table})
    where seq = 1;
    """

    try:
        cur.execute(query)
        print(f"Data copied from {schema}.{source_table} to {schema}.{new_table}.")

    except psycopg2.Error as e:
        print(f"Error occurred: {e}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # create dummy table for test
    conn = get_Redshift_connection(True)
    create_dummy_table(conn, REDSHIFT_USER, "stock_info_dummy")
    create_dummy_table(conn, REDSHIFT_USER, "stock_info_dummy_2")

    # insert dummy data
    records = get_historical_prices("AAPL")
    insert_dummy_data_into_redshift(
        REDSHIFT_USER,
        "stock_info_dummy",
        records,
        datetime.datetime(2023, 5, 5, 0, 0, 0),
    )
    insert_dummy_data_into_redshift(
        REDSHIFT_USER,
        "stock_info_dummy",
        records,
        datetime.datetime(2023, 6, 6, 0, 0, 0),
    )
    insert_dummy_data_into_redshift(
        REDSHIFT_USER,
        "stock_info_dummy",
        records,
        datetime.datetime(2023, 7, 7, 0, 0, 0),
    )

    # test SELECT query
    select_query = f"""
    select * from
    (select
        *,
        row_number() over (partition by date order by created_date desc) seq
    from
    {REDSHIFT_USER}.stock_info_dummy)
    where seq = 1;
    """
    get_data_from_redshift(select_query)

    # test INSERT query
    copy_dummy_data_into_redshift(
        REDSHIFT_USER, "stock_info_dummy", "stock_info_dummy_2"
    )
    get_data_from_redshift(f"select * from {REDSHIFT_USER}.stock_info_dummy_2;")
