# full refresh 기준으로는 : 기존 데이터 있으면 삭제 -> 다시 적재
import os
import requests
import psycopg2


# 기존 함수
def extract(url):
    f = requests.get(url)
    return f.text


def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = os.environ.get("REDSHIFT_USER")
    redshift_pass = os.environ.get("REDSHIFT_PASSWORD")
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(
        "dbname={dbname} user={user} host={host} password={password} port={port}".format(
            dbname=dbname,
            user=redshift_user,
            password=redshift_pass,
            host=host,
            port=port,
        )
    )

    conn.set_session(autocommit=False)
    return conn


# 수정 및 추가 함수
def transform(text):
    lines = text.strip().split("\n")
    records = []
    for idx, data in enumerate(lines):
        if idx != 0:
            (name, gender) = data.split(",")  # l = "Keeyong,M" -> [ 'keeyong', 'M' ]
            records.append([name, gender])
    return records


def refresh_table(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM leekh090163.name_gender")
    count = cur.fetchone()[0]

    if count != 0:
        try:
            cur.execute("DELETE FROM TABLE leekh090163.name_gender")
            # 트랜잭션 커밋
            conn.commit()
            print("테이블 refresh 완료")

        except psycopg2.Error as e:
            # 트랜잭션 롤백
            conn.rollback()
            print("테이블 truncate 중 오류 발생:", e)
            raise

        finally:
            cur.close()
            conn.close()

    else:
        print("테이블에 데이터가 없습니다.")
        cur.close()
        conn.close()


def insert_record(conn, sql):
    cur = conn.cursor()

    try:
        cur.execute(sql)
        conn.commit()

    except psycopg2.Error as e:
        print("insert 중 오류 발생:", e)
        raise


def load(records):
    """
    records = [
      [ "Name", "Gender" ]
      [ "Keeyong", "M" ],
      [ "Claire", "F" ],
      ...
    ]
    """
    # refresh table(tranaction 1)
    conn = get_Redshift_connection()
    refresh_table(conn)

    # insert new data to table(transaction 2)
    conn = get_Redshift_connection()

    for r in records:
        name = r[0]
        gender = r[1]
        sql = f"INSERT INTO leekh090163.name_gender VALUES ('{name}', '{gender}')"
        insert_record(conn, sql)
        # print(sql)

    print("모든 데이터가 적재되었습니다.")
    conn.close()


def test():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)

    lines = transform(data)
    pure_data_length = len(lines)

    for i in range(5):
        load(lines)
