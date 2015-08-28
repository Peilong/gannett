drop table work.realized_segments_idfa_oneday;
create table work.realized_segments_idfa_oneday as
select a.idfa, r.* from work.realizedsegments_oneday r left join
ADOBE_IDFA.AdobeIDFA a on a.adobe_id = r.uuid;
