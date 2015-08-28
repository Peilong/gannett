
drop table work.realized_segments_idfa_rollup_ip_oneday ;
create table work.realized_segments_idfa_rollup_ip_oneday as
select min(eventtime) as min_event_tm, max(eventtime) as max_event_tm,
count(uuid) as cnt, count(distinct ip) as dist_cnt_ip, idfa, uuid from work.realized_segments_idfa_oneday group by idfa, uuid;

