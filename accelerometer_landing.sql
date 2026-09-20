CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_project`.`accelerometer_landing` (
  `user` string,
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float
) COMMENT "Accelerometer data from the landing zone"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-d609-project-2026/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');