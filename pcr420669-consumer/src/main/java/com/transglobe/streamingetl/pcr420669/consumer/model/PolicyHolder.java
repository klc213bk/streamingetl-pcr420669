package com.transglobe.streamingetl.pcr420669.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PolicyHolder {
	
	@JsonProperty("LIST_ID")
	private Long listId;
	
	@JsonProperty("POLICY_ID")
	private Long policyId;
	
	@JsonProperty("NAME")
	private String name;
	
	@JsonProperty("CERTI_CODE")
	private String certiCode;
	
	@JsonProperty("MOBILE_TEL")
	private String mobileTel;
	
	@JsonProperty("EMAIl")
	private String email;
	
	@JsonProperty("ADDRESS_ID")
	private Long addressId;

	public Long getListId() {
		return listId;
	}

	public void setListId(Long listId) {
		this.listId = listId;
	}

	public Long getPolicyId() {
		return policyId;
	}

	public void setPolicyId(Long policyId) {
		this.policyId = policyId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCertiCode() {
		return certiCode;
	}

	public void setCertiCode(String certiCode) {
		this.certiCode = certiCode;
	}

	public String getMobileTel() {
		return mobileTel;
	}

	public void setMobileTel(String mobileTel) {
		this.mobileTel = mobileTel;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Long getAddressId() {
		return addressId;
	}

	public void setAddressId(Long addressId) {
		this.addressId = addressId;
	}
	
	
	
	/*
	@JsonProperty("LIST_ID")
	private Long listId;
	
	@JsonProperty("POLICY_ID")
	private Long policyId;
	
	@JsonProperty("APPLICANT_AGE")
	private Integer applicantAge;
	
	@JsonProperty("ACTIVE_STATUS")
	private String activeStatus;
	
	@JsonProperty("PARTY_ID")
	private Long partyId;
	
	@JsonProperty("BIRTH_DATE")
	private Date birthDate;
	
	@JsonProperty("EM_VALUE")
	private Integer emsValue;
	
	@JsonProperty("JOB_CLASS")
	private Integer jobClass;
	
	@JsonProperty("JOB_1")
	private String job1;
	
	@JsonProperty("JOB_2")
	private String job2;
	
	@JsonProperty("RELATION_TO_LA")
	private Integer relationToLa;
	
	@JsonProperty("RELIGION_CODE")
	private String religionCode;
	
	@JsonProperty("TELEPHONE")
	private String telephone;
	
	@JsonProperty("MOBILE_TEL")
	private String mobileTel;
	
	@JsonProperty("ADDRESS_ID")
	private Long addressId;
	
	@JsonProperty("JOB_CATE_ID")
	private Long jobCateId;
	
	@JsonProperty("EMS_VERSION")
	private Long emsVersion;
	
	@JsonProperty("INSERTED_BY")
	private Long insertedby;
	
	@JsonProperty("UPDATED_BY")
	private Long updatedBy;
	
	@JsonProperty("INSERT_TIME")
	private Date insertTime;
	
	@JsonProperty("UPDATE_TIME")
	private Date updateTime;
	
	@JsonProperty("INSERT_TIMESTAMP")
	private Date insertTimestamp;
	
	@JsonProperty("UPDATE_TIMESTAMP")
	private Date updateTimestamp;
	
	@JsonProperty("PARTY_TYPE")
	private String partyType;
	
	@JsonProperty("FATCA_INDI")
	private String fatcaIndi;
	
	@JsonProperty("NAME")
	private String name;
	
	@JsonProperty("CERTI_CODE")
	private String certiCode;
	
	@JsonProperty("CERTI_TYPE")
	private Integer certiType;
	
	@JsonProperty("GENDER")
	private String gender;
	
	@JsonProperty("OFFICE_TEL_REG")
	private String officeTelReg;
	
	@JsonProperty("OFFICE_TEL")
	private String officeTel;
	
	@JsonProperty("OFFICE_TEL_EXT")
	private String officeTelExt;
	
	@JsonProperty("HOME_TEL_REG")
	private String homeTelReg;
	
	@JsonProperty("HOME_TEL")
	private String homeTel;
	
	@JsonProperty("HOME_TEL_EXT")
	String homeTelExt;
	
	@JsonProperty("HEIGHT")
	private BigDecimal height;
	
	@JsonProperty("WEIGHT")
	private BigDecimal weight;
	
	@JsonProperty("BMI")
	private Integer bmi;
	
	@JsonProperty("NOTIFY_INDI")
	private String notifyIndi;
	
	@JsonProperty("PREGNANCY_WEEK")
	private Integer pregnancyWeek;
	
	@JsonProperty("EMAIL")
	private String email;
	
	@JsonProperty("MOBILE_TEL2")
	private String mobileTel2;
	
	@JsonProperty("LEADER_NAME")
	private String leaderName;
	
	@JsonProperty("POLICY_CUST_ID")
	private Long policyCustId;
	
	@JsonProperty("NATIONALITY")
	private String nationality;
	
	@JsonProperty("AUTHORIZATION_NO")
	private String authorizationNo;
	
	@JsonProperty("OFFICE_TEL_NATIONAL")
	private String officeTelNational;
	
	@JsonProperty("HOME_TEL_NATIONAL")
	private String homeTelNational;
	
	@JsonProperty("MOBILE_TEL_NATIONAL")
	private String mobileTelNational;
	
	@JsonProperty("MOBILE_TEL2_NATIONAL")
	private String mobileTel2National;
	
	@JsonProperty("ACC_ENGLISH_NAME")
	private String accEnglishName;
	
	@JsonProperty("ENGLISH_BANK_NAME")
	private String englishBankName;
	
	@JsonProperty("OIU_BANK_ACCOUNT")
	String oiuBankAccount;
	
	@JsonProperty("OIU_ACCOUNT_ID")
	private Long oiubankId;
	
	@JsonProperty("COMPANY_CATEGORY")
	private Long companyCategory;
	
	@JsonProperty("ENTRY_DATE")
	private Date entryDate;
	
	@JsonProperty("BANK_CODE")
	private String bankCode;
	
	@JsonProperty("BRANCH_CODE")
	private String branchCode;
	
	@JsonProperty("MARRIAGE_ID")
	private String marriageId;
	
	@JsonProperty("NATIONALITY2")
	private String nationality2;
	
	@JsonProperty("ENGLISH_NAME")
	private String englishName;
	
	@JsonProperty("ROMAN_NAME")
	private String romanName;
	
	@JsonProperty("ABANDON_INHERITANCE")
	private String abandonInheritance;
	
	@JsonProperty("COUNTER_SERVICES")
	private String counterServices;
	
	@JsonProperty("LEADER_ROMAN_NAME")
	private String leaderRomanName;
	
	@JsonProperty("GUARDIAN_ANCMNT")
	private String guardianAncmnt;
	
	@JsonProperty("OLD_FOREIGNER_ID")
	private String oldForeignerId;

	*/
	
}
