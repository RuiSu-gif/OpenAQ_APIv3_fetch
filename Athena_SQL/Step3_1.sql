CREATE EXTERNAL TABLE `openaqMeasurements`(
  `location_id` INT,
  `sensors_id` INT,
  `location` STRING,
  `datetime` STRING,
  `lat` float,
  `lon` float,
  `parameter` STRING,
  `units` STRING,
  `value` float
)
PARTITIONED BY (locationid string, year string, month string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
LOCATION
  's3://openaq-data-archive/records/csv.gz/'
  TBLPROPERTIES ('skip.header.line.count'='1')