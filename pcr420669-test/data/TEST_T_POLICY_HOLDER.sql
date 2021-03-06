CREATE TABLE "TEST_T_POLICY_HOLDER" 
   (	"LIST_ID" NUMBER(19,0) NOT NULL, 
	"POLICY_ID" NUMBER(19,0) NOT NULL, 
	"APPLICANT_AGE" NUMBER(3,0), 
	"ACTIVE_STATUS" VARCHAR2(1 BYTE) DEFAULT '1' NOT NULL, 
	"PARTY_ID" NUMBER(19,0) NOT NULL, 
	"BIRTH_DATE" DATE, 
	"EM_VALUE" NUMBER(4,0) DEFAULT 0, 
	"JOB_CLASS" NUMBER(2,0), 
	"JOB_1" VARCHAR2(255 BYTE), 
	"JOB_2" VARCHAR2(255 BYTE), 
	"RELATION_TO_LA" NUMBER(4,0), 
	"RELIGION_CODE" VARCHAR2(3 BYTE), 
	"TELEPHONE" VARCHAR2(40 BYTE), 
	"MOBILE_TEL" VARCHAR2(40 BYTE), 
	"ADDRESS_ID" NUMBER(19,0), 
	"JOB_CATE_ID" NUMBER(19,0), 
	"EMS_VERSION" NUMBER(10,0) DEFAULT 0 NOT NULL, 
	"INSERTED_BY" NUMBER(19,0) NOT NULL, 
	"UPDATED_BY" NUMBER(19,0) NOT NULL, 
	"INSERT_TIME" DATE NOT NULL, 
	"UPDATE_TIME" DATE NOT NULL, 
	"INSERT_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"UPDATE_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"PARTY_TYPE" VARCHAR2(2 BYTE), 
	"FATCA_INDI" CHAR(1 BYTE), 
	"NAME" VARCHAR2(300 BYTE), 
	"CERTI_CODE" VARCHAR2(50 BYTE), 
	"CERTI_TYPE" NUMBER(2,0), 
	"GENDER" CHAR(1 BYTE), 
	"OFFICE_TEL_REG" VARCHAR2(4 BYTE), 
	"OFFICE_TEL" VARCHAR2(40 BYTE), 
	"OFFICE_TEL_EXT" VARCHAR2(10 BYTE), 
	"HOME_TEL_REG" VARCHAR2(4 BYTE), 
	"HOME_TEL" VARCHAR2(40 BYTE), 
	"HOME_TEL_EXT" VARCHAR2(10 BYTE), 
	"HEIGHT" NUMBER(5,2), 
	"WEIGHT" NUMBER(5,2), 
	"BMI" NUMBER(4,0), 
	"NOTIFY_INDI" CHAR(1 BYTE), 
	"PREGNANCY_WEEK" NUMBER(2,0), 
	"EMAIL" VARCHAR2(100 BYTE), 
	"MOBILE_TEL2" VARCHAR2(40 BYTE), 
	"LEADER_NAME" VARCHAR2(210 BYTE), 
	"POLICY_CUST_ID" NUMBER(19,0), 
	"NATIONALITY" VARCHAR2(3 BYTE), 
	"AUTHORIZATION_NO" VARCHAR2(10 BYTE), 
	"OFFICE_TEL_NATIONAL" VARCHAR2(4 BYTE), 
	"HOME_TEL_NATIONAL" VARCHAR2(4 BYTE), 
	"MOBILE_TEL_NATIONAL" VARCHAR2(4 BYTE), 
	"MOBILE_TEL2_NATIONAL" VARCHAR2(4 BYTE), 
	"ACC_ENGLISH_NAME" VARCHAR2(70 BYTE), 
	"ENGLISH_BANK_NAME" VARCHAR2(70 BYTE), 
	"OIU_BANK_ACCOUNT" VARCHAR2(14 BYTE), 
	"OIU_ACCOUNT_ID" NUMBER(19,0), 
	"COMPANY_CATEGORY" VARCHAR2(5 BYTE), 
	"ENTRY_DATE" DATE, 
	"BANK_CODE" VARCHAR2(20 BYTE), 
	"BRANCH_CODE" VARCHAR2(20 BYTE), 
	"MARRIAGE_ID" VARCHAR2(1 BYTE), 
	"NATIONALITY2" VARCHAR2(3 BYTE), 
	"ENGLISH_NAME" VARCHAR2(500 BYTE), 
	"ROMAN_NAME" VARCHAR2(300 BYTE), 
	"ABANDON_INHERITANCE" CHAR(1 BYTE), 
	"COUNTER_SERVICES" CHAR(1 BYTE), 
	"LEADER_ROMAN_NAME" VARCHAR2(300 BYTE), 
	"GUARDIAN_ANCMNT" CHAR(1 BYTE), 
	"OLD_FOREIGNER_ID" VARCHAR2(50 BYTE), 
	 CONSTRAINT "PK_TEST_T_POLICY_HOLDER" PRIMARY KEY ("LIST_ID")
);
	 