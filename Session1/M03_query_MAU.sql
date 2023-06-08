--query
select 
to_char(date_trunc('month', ts), 'yyyy-mm') as month,
count(distinct userid) as "MAU" 
from 
raw_data.session_timestamp a
left join raw_data.user_session_channel b
on a.sessionid=b.sessionid
group by month
order by month;

--memo
--timestamp group by에 date_trunc, timestamp_trunc 함수 사용 가능(timestamp_trunc의 경우 postgres 9.1 이상 버전에만 지원)
--date_trunc를 쓰더라도 column명칭으로 년-월(yyyy-mm) 형식으로 나오게 하려면 to_char로 감싸줘야 함
--colab에서는 column명을 대문자로 지정하더라도(ex."MAU") 출력은 소문자