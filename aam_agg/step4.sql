drop table work.realized_segments_idfa_segment_detail_oneday;
create table work.realized_segments_idfa_segment_detail_oneday as select s.*,
 `Integration_Code`,
  `data_source_id`,
  `name`,
  `z7_day_real_time` ,
  `z14_day_real_time` ,
  `z30_day_real_time` ,
  `z60_day_real_time` ,
  `z7_day_total` ,
  `z14_day_total` ,
  `z30_day_total` ,
  `z60_day_total`
from work.realized_segments_idfa_oneday s left join Adobe_Reference.Segments_lookup l on s.realized_segments = l.sid;

