

drop table work.realizedsegments_oneday;
create table work.realizedsegments_oneday as
select eventtime,uuid,ip,referer, marketing_cloud_id,
explodkey as realized_segments from adobe_audience.adobe_audience_manager_history lateral view explode(realizedsegments)
fooTab as explodkey;
