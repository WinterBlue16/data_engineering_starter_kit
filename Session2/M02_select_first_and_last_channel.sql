SELECT
    DISTINCT 
    userid,
    first_value(channel) OVER (--첫 번째 값
        PARTITION BY userid --userid별로 파티션 나누기 
        ORDER BY seq --seq를 기준으로 오름차순 정렬
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW --파티션 시작부터 현재 행까지 범위 지정
        ) AS first_channel,
    last_value(channel) OVER (--마지막 값
        PARTITION BY userid
        ORDER BY seq
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING --현재 행부터 파티션 끝까지 범위 지정
        ) AS last_channel
FROM
(
    SELECT 
        userid, 
        channel, 
        ts, 
        ROW_NUMBER() OVER (partition by userid order by ts ASC) seq 
    FROM raw_data.user_session_channel a
    LEFT JOIN raw_data.session_timestamp b
    ON a.sessionid=b.sessionid
);