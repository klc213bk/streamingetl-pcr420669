create table T_STREAMING_ETL_HEALTH
(
	id NUMBER(19, 0),
	cdc_time timestamp,
	logminer_id VARCHAR(20),
	logminer_time timestamp,
	consumer_id VARCHAR(20),
	consumer_time timestamp,
	primary key (id)
) WITH "template=REPLICATED,backups=0,CACHE_NAME=StreamingEtlHealth, ATOMICITY=ATOMIC";