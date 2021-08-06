DROP TABLE IF EXISTS station_mart;

create external table if not exists station_mart(bikes_available int, docks_available int, is_renting boolean, is_returning boolean, last_updated_epoch double, station_id string, name string, latitude double, longitude double, last_updated timestamp)
comment 'table to visualise station marts'
row format delimited
fields terminated by ','
stored as textfile
location '/tw/stationMart/data'
TBLPROPERTIES ("skip.header.line.count"="1");

create view if not exists final_view as (select * from station_mart join  (select station_id as id, max(last_updated_epoch) as max_time FROM station_mart GROUP BY station_id) tmp on (tmp.id = station_mart.station_id) where station_mart.last_updated_epoch = tmp.max_time);

--create view if not exists tmp as (select * from test_latest join  (select station_id as id, max(last_updated_epoch) as max_time FROM test_latest GROUP BY station_id) tmp on (tmp.id = test_latest.station_id) where test_latest.last_updated_epoch = tmp.max_time);

-- see how to convert field to timestamp