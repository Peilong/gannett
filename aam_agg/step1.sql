drop table work.requestparameters_oneday;
create table work.requestparameters_oneday as
select distinct eventtime,uuid,explodkey as request_type, explodval as detail,referer, ip,marketing_cloud_id  from adobe_audience.adobe_audience_manager_history lateral view explode(requestparameters)
fooTab as explodkey,explodval ;
