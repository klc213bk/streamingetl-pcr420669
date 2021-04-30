package com.transglobe.streamingetl.pcr420669.consumer.model;

import java.math.BigDecimal;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PolicyHolder {
	@JsonProperty("LIST_ID")
	Long listId;
	
	@JsonProperty("POLICY_ID")
	Long policyId;
	
	@JsonProperty("APPLICANT_AGE")
	Integer applicantAge;
	
	@JsonProperty("ACTIVE_STATUS")
	String activeStatus;
	
	@JsonProperty("PARTY_ID")
	Long partyId;
	
	@JsonProperty("BIRTH_DATE")
	Date birthDate;
	
	@JsonProperty("EM_VALUE")
	Integer emsValue;
	
	@JsonProperty("JOB_CLASS")
	Integer jobClass;
	
	@JsonProperty("JOB_1")
	String job1;
	
	@JsonProperty("JOB_2")
	String job2;
	
	@JsonProperty("RELATION_TO_LA")
	Integer relationToLa;
	
	@JsonProperty("RELIGION_CODE")
	String religionCode;
	
	@JsonProperty("TELEPHONE")
	String telephone;
	
	@JsonProperty("MOBILE_TEL")
	String mobileTel;
	
	@JsonProperty("ADDRESS_ID")
	Long addressId;
	
	@JsonProperty("JOB_CATE_ID")
	Long jobCateId;
	
	@JsonProperty("EMS_VERSION")
	Long emsVersion;
	
	@JsonProperty("INSERTED_BY")
	Long insertedby;
	
	@JsonProperty("UPDATED_BY")
	Long updatedBy;
	
	@JsonProperty("INSERT_TIME")
	Date insertTime;
	
	@JsonProperty("UPDATE_TIME")
	Date updateTime;
	
	@JsonProperty("INSERT_TIMESTAMP")
	Date insertTimestamp;
	
	@JsonProperty("UPDATE_TIMESTAMP")
	Date updateTimestamp;
	
	@JsonProperty("PARTY_TYPE")
	String partyType;
	
	@JsonProperty("FATCA_INDI")
	String fatcaIndi;
	
	@JsonProperty("NAME")
	String name;
	
	@JsonProperty("CERTI_CODE")
	String certiCode;
	
	@JsonProperty("CERTI_TYPE")
	Integer certiType;
	
	@JsonProperty("GENDER")
	String gender;
	
	@JsonProperty("OFFICE_TEL_REG")
	String officeTelReg;
	
	@JsonProperty("OFFICE_TEL")
	String officeTel;
	
	@JsonProperty("OFFICE_TEL_EXT")
	String officeTelExt;
	
	@JsonProperty("HOME_TEL_REG")
	String homeTelReg;
	
	@JsonProperty("HOME_TEL")
	String homeTel;
	
	@JsonProperty("HOME_TEL_EXT")
	String homeTelExt;
	
	@JsonProperty("HEIGHT")
	BigDecimal height;
	
	@JsonProperty("WEIGHT")
	BigDecimal weight;
	
	@JsonProperty("BMI")
	Integer bmi;
	
	@JsonProperty("NOTIFY_INDI")
	String notifyIndi;
	
	@JsonProperty("PREGNANCY_WEEK")
	Integer pregnancyWeek;
	
	@JsonProperty("EMAIL")
	String email;
	
	@JsonProperty("MOBILE_TEL2")
	String mobileTel2;
	
	@JsonProperty("LEADER_NAME")
	String leaderName;
	
	@JsonProperty("POLICY_CUST_ID")
	Long policyCustId;
	
	@JsonProperty("NATIONALITY")
	String nationality;
	
	@JsonProperty("AUTHORIZATION_NO")
	String authorizationNo;
	
	@JsonProperty("OFFICE_TEL_NATIONAL")
	String officeTelNational;
	
	@JsonProperty("HOME_TEL_NATIONAL")
	String homeTelNational;
	
	@JsonProperty("MOBILE_TEL_NATIONAL")
	String mobileTelNational;
	
	@JsonProperty("MOBILE_TEL2_NATIONAL")
	String mobileTel2National;
	
	@JsonProperty("ACC_ENGLISH_NAME")
	String accEnglishName;
	
	@JsonProperty("ENGLISH_BANK_NAME")
	String englishBankName;
	
	@JsonProperty("OIU_BANK_ACCOUNT")
	String oiuBankAccount;
	
	@JsonProperty("OIU_ACCOUNT_ID")
	Long oiubankId;
	
	@JsonProperty("COMPANY_CATEGORY")
	Long companyCategory;
	
	@JsonProperty("ENTRY_DATE")
	Date entryDate;
	
	@JsonProperty("BANK_CODE")
	String bankCode;
	
	@JsonProperty("BRANCH_CODE")
	String branchCode;
	
	@JsonProperty("MARRIAGE_ID")
	String marriageId;
	
	@JsonProperty("NATIONALITY2")
	String nationality2;
	
	@JsonProperty("ENGLISH_NAME")
	String englishName;
	
	@JsonProperty("ROMAN_NAME")
	String romanName;
	
	@JsonProperty("ABANDON_INHERITANCE")
	String abandonInheritance;
	
	@JsonProperty("COUNTER_SERVICES")
	String counterServices;
	
	@JsonProperty("LEADER_ROMAN_NAME")
	String leaderRomanName;
	
	@JsonProperty("GUARDIAN_ANCMNT")
	String guardianAncmnt;
	
	@JsonProperty("OLD_FOREIGNER_ID")
	String oldForeignerId;
}
