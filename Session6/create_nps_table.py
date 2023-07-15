import os
import psycopg2

# constants
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST")
REDSHIFT_USER = os.environ.get("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")


def get_Redshift_connection(autocommit: bool = False):
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


def create_table_to_redshift(conn, schema, table):
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id INT NOT NULL primary key,
            created_at timestamp,
            score smallint
        );"""
        )
        cur.execute("COMMIT;")
        print(f"Table {schema}.{table} created successfully!")

    except psycopg2.Error as e:
        print(f"Error occurred: {e}")
        cur.execute("ROLLBACK;")

    finally:
        cur.close()
        conn.close()


def main():
    conn = get_Redshift_connection(True)
    create_table_to_redshift(conn, REDSHIFT_USER, "nps")


if __name__ == "__main__":
    main()
