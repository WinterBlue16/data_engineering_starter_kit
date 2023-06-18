select
    userid,
    sum(amount) as gross_revenue
from
(
    select *
    from raw_data.user_session_channel a
    left join raw_data.session_timestamp b on a.sessionid=b.sessionid
    left join raw_data.session_transaction c on b.sessionid=c.sessionid
    
)
group by userid
order by gross_revenue desc;
