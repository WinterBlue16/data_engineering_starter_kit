select
    to_char(ts, 'yyyy-mm') as "yearMonth",
    channel,
    count(distinct userid) as "uniqueUsers",
    count(distinct case when amount is not null then userid end) as "paidUsers",
    round(cast("paidUsers" as float)/nullif(cast("uniqueUsers" as float),0),2) as "conversionRate",
    sum(coalesce(amount,0)) "grossRevenue",
    sum(case when refunded=false then amount else 0 end) as "netRevenue" --환불되지 않은 경우에만 계산
from
(
    select
        a.userid,
        a.sessionid,
        a.channel,
        b.ts,
        c.refunded,
        c.amount
    from raw_data.user_session_channel a
    left join raw_data.session_timestamp b on a.sessionid=b.sessionid
    left join raw_data.session_transaction c on b.sessionid=c.sessionid
    ) as subquery 
group by "yearMonth", channel
order by channel;