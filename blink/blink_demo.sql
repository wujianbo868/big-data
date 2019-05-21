--SQL
--********************************************************************--
--Author: 罗心
--CreateTime: 2019-05-17 17:09:05
--Comment: 统计指定周期内各注册方式注册的量
--********************************************************************--

create table sls_stream(
  registerType varchar,
  ytid bigint,
  logtime timestamp,
  watermark for logtime as withOffset(logtime,0)
) with (
  type ='sls',
endPoint ='xxxxxxxxxx',
accessId ='xxxxxxxx',
accessKey ='xxxxxxxxxx',
startTime = '2019-05-21 13:00:00',
project ='xxxxxx',
logStore ='xxxxxxxxx',
consumerGroup='consumerGroupTest1'å
);

create table blink_register_type_count (
  register_type varchar,
  num bigint,
  window_start timestamp,
  window_end timestamp
) with (
    type = 'odps',
endPoint = 'xxxxxxxxx',
project = 'xxxxxxxx',
tableName = 'xxxxxxx',
accessId = 'xxxxxxx',
accessKey = 'xxxxxxxx'
);

insert into blink_register_type_count
  select
  registerType as register_type,
  count(1) as num,
  tumble_start(logtime,  INTERVAL '60' MINUTE),
  tumble_end(logtime,  INTERVAL '60' MINUTE)
  from sls_stream
  group by tumble(logtime, INTERVAL '60' MINUTE), registerType