CREATE TABLE T_PARTY_CONTACT
   (	
   ROLE_TYPE NUMBER(1,0) NOT NULL, 
	LIST_ID NUMBER(19,0) NOT NULL, 
	POLICY_ID NUMBER(19,0), 
	NAME VARCHAR2(300 BYTE), 
	CERTI_CODE VARCHAR2(50 BYTE), 
	MOBILE_TEL VARCHAR2(40 BYTE), 
	EMAIL VARCHAR2(100 BYTE), 
	ADDRESS_ID NUMBER(19,0), 
	ADDRESS_1 VARCHAR2(600 BYTE), 
	 PRIMARY KEY (ROLE_TYPE,LIST_ID)
) WITH "template=REPLICATED,backups=0,CACHE_NAME=PartyContact, ATOMICITY=ATOMIC";


