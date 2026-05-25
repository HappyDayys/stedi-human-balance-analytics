CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
PARTITIONED BY ( 
  `partition_0` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='distanceFromObject,sensorReadingTime,serialNumber') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-sthiti/step_trainer_landing/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='70dcfdd4-6df2-4910-aae2-b5dd1bb58f4a', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='step_trainer_landing_crawler', 
  'averageRecordSize'='1032', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='3', 
  'partition_filtering.enabled'='true', 
  'recordCount'='3194', 
  'sizeKey'='3298200', 
  'typeOfData'='file')
