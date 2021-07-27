create table T_LOGMINER_SCN_SINK
(
    id  NUMBER(19, 0),
	streaming_name VARCHAR2(30),
	scn NUMBER(19, 0),
	scn_insert_time TIMESTAMP,
	scn_update_time TIMESTAMP,
	health_time TIMESTAMP,
	primary key (id)
) WITH "template=REPLICATED,backups=0,CACHE_NAME=LogminerScnSink, ATOMICITY=ATOMIC";

	