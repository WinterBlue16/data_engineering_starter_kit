{
    "table": "channel_summary",
    "schema": "leekh090163",
    "main_sql": """SELECT
 DISTINCT A.userid,
 FIRST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and
unbounded following) AS First_Channel,
 LAST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and
unbounded following) AS Last_Channel
 FROM raw_data.user_session_channel A
 LEFT JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid;""",
    # user_session_channel 테이블과 session_timestamp 테이블 데이터 존재 여부 체크
    "input_check": [
        {
            "sql": "SELECT COUNT(1) FROM leekh090163.user_session_channel",
            "count": 1,
        },
        {
            "sql": "SELECT COUNT(1) FROM leekh090163.session_timestamp",
            "count": 1,
        },
    ],
    # temp_channel_summary 테이블 데이터 존재 여부 체크
    "output_check": [{"sql": "SELECT COUNT(1) FROM {schema}.temp_{table}", "count": 1}],
}
