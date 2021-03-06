CREATE TABLE "TEST_T_ADDRESS" 
   (	"ADDRESS_ID" NUMBER(19,0) NOT NULL ENABLE NOVALIDATE, 
	"ADDRESS_1" VARCHAR2(600 BYTE), 
	"ADDRESS_2" VARCHAR2(200 BYTE), 
	"ADDRESS_3" VARCHAR2(500 BYTE), 
	"ADDRESS_4" VARCHAR2(100 BYTE), 
	"ADDRESS_5" VARCHAR2(100 BYTE), 
	"ADDRESS_6" VARCHAR2(100 BYTE), 
	"ADDRESS_7" VARCHAR2(100 BYTE), 
	"ADDRESS_FORMAT" VARCHAR2(1 BYTE) NOT NULL ENABLE NOVALIDATE, 
	"POST_CODE" VARCHAR2(10 BYTE), 
	"STATE" VARCHAR2(100 BYTE), 
	"COUNTRY_CODE" VARCHAR2(3 BYTE), 
	"ADDRESS_STATUS" VARCHAR2(1 BYTE) DEFAULT 1 NOT NULL ENABLE NOVALIDATE, 
	"FOREIGN_INDI" CHAR(1 BYTE), 
	"UPDATE_DATE" DATE NOT NULL ENABLE NOVALIDATE, 
	"OPERATOR_ID" NUMBER(19,0), 
	"OPERATOR_DEPT_ID" NUMBER(19,0), 
	"INSERT_DATE" DATE DEFAULT sysdate NOT NULL ENABLE NOVALIDATE, 
	"ADDRESS_FORMAT_TYPE" VARCHAR2(1 BYTE), 
	"MAIL_FAILED_TIMES" NUMBER(10,0) DEFAULT null, 
	"RETURN_MAIL_DATE" DATE, 
	"RETURN_MAIL_REASON" VARCHAR2(2 BYTE), 
	"INSERT_TIME" DATE NOT NULL ENABLE NOVALIDATE, 
	"RECORDER_ID" NUMBER(19,0) NOT NULL ENABLE NOVALIDATE, 
	"UPDATE_TIME" DATE NOT NULL ENABLE NOVALIDATE, 
	"UPDATER_ID" NUMBER(19,0) NOT NULL ENABLE NOVALIDATE, 
	"VALIDITY_START_DATE" DATE, 
	"CITY" VARCHAR2(50 BYTE), 
	"INSERT_TIMESTAMP" DATE DEFAULT sysdate NOT NULL ENABLE NOVALIDATE, 
	"UPDATE_TIMESTAMP" DATE DEFAULT sysdate NOT NULL ENABLE NOVALIDATE, 
	"ADDRESS_8" VARCHAR2(100 BYTE), 
	"ADDRESS_9" VARCHAR2(100 BYTE), 
	"ADDRESS_10" VARCHAR2(100 BYTE), 
	"ADDRESS_11" VARCHAR2(100 BYTE), 
	"ADDRESS_12" VARCHAR2(100 BYTE), 
	"TELEPHONE" VARCHAR2(30 BYTE), 
	"FAX" VARCHAR2(30 BYTE), 
	"BOX_TYPE" VARCHAR2(2 BYTE), 
	"NATIONALITY" VARCHAR2(3 BYTE), 
	 CONSTRAINT "PK_TEST_T_ADDRESS" PRIMARY KEY ("ADDRESS_ID")
);
