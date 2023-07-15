{
    "table": "mau_summary",
    "schema": "leekh090163",
    "main_sql": """SELECT 
  TO_CHAR(A.ts, 'YYYY-MM') AS month,
  COUNT(DISTINCT B.userid) AS mau
FROM raw_data.session_timestamp A
JOIN raw_data.user_session_channel B ON A.sessionid = B.sessionid
GROUP BY 1 
;""",
    # user_session_channel 테이블과 session_timestamp 테이블 데이터 존재 여부 체크
    "input_check": [
        {"sql": "SELECT COUNT(1) FROM leekh090163.session_timestamp", "count": 1},
        {"sql": "SELECT COUNT(1) FROM leekh090163.user_session_channel", "count": 1},
    ],
    # temp_channel_summary 테이블 데이터 존재 여부 체크
    "output_check": [{"sql": "SELECT COUNT(1) FROM {schema}.temp_{table}", "count": 1}],
}
