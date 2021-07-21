create table T_STREAMING_REGISTER
(
	time NUMBER(19, 0),
	current_scn NUMBER(19, 0),
	remark VARCHAR2(1000),
	primary key (time)
)
