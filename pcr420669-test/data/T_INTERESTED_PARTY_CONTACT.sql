CREATE TABLE "T_INTERESTED_PARTY_CONTACT" 
   (	
   "SEQ_NO" NUMBER(19,0) NOT NULL, 
   "ROLE_TYPE" NUMBER(1,0) NOT NULL, 
	"LIST_ID" NUMBER(19,0) NOT NULL, 
	"POLICY_ID" NUMBER(19,0) NOT NULL, 
	"NAME" VARCHAR2(300 BYTE), 
	"CERTI_CODE" VARCHAR2(50 BYTE), 
	"MOBILE_TEL" VARCHAR2(40 BYTE), 
	"EMAIL" VARCHAR2(100 BYTE), 
	"ADDRESS_ID" NUMBER(19,0), 
	"ADDRESS_1" VARCHAR2(600 BYTE), 
	 PRIMARY KEY ("SEQ_NO")
);

COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.ROLE_TYPE IS '保單關係人種類, 1: 要保人,2: 被保人, 3: 受益人'; 
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.LIST_ID IS 'Sequence ID';
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.POLICY_ID IS '保單 ID';
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.NAME IS '姓名';
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.CERTI_CODE IS '身分證號/統一編(證)號';
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.MOBILE_TEL IS '手機號碼';
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.EMAIL IS 'Email';
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.ADDRESS_ID IS 'Address id';
COMMENT ON COLUMN T_INTERESTED_PARTY_CONTACT.ADDRESS_1 IS '地址';
COMMENT ON TABLE T_INTERESTED_PARTY_CONTACT  IS '保單關係人聯絡資訊';

CREATE INDEX IDX_INTERESTED_PARTY_CONTACT1 ON T_INTERESTED_PARTY_CONTACT ("MOBILE_TEL");
CREATE INDEX IDX_INTERESTED_PARTY_CONTACT2 ON T_INTERESTED_PARTY_CONTACT ("EMAIL");
