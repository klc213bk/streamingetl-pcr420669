package com.transglobe.streamingetl.pcr420669.consumer.model;

import java.math.BigDecimal;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PolicyHolder {
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

	public Integer getApplicantAge() {
		return applicantAge;
	}

	public void setApplicantAge(Integer applicantAge) {
		this.applicantAge = applicantAge;
	}

	public String getActiveStatus() {
		return activeStatus;
	}

	public void setActiveStatus(String activeStatus) {
		this.activeStatus = activeStatus;
	}

	public Long getPartyId() {
		return partyId;
	}

	public void setPartyId(Long partyId) {
		this.partyId = partyId;
	}

	public Date getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

	public Integer getEmsValue() {
		return emsValue;
	}

	public void setEmsValue(Integer emsValue) {
		this.emsValue = emsValue;
	}

	public Integer getJobClass() {
		return jobClass;
	}

	public void setJobClass(Integer jobClass) {
		this.jobClass = jobClass;
	}

	public String getJob1() {
		return job1;
	}

	public void setJob1(String job1) {
		this.job1 = job1;
	}

	public String getJob2() {
		return job2;
	}

	public void setJob2(String job2) {
		this.job2 = job2;
	}

	public Integer getRelationToLa() {
		return relationToLa;
	}

	public void setRelationToLa(Integer relationToLa) {
		this.relationToLa = relationToLa;
	}

	public String getReligionCode() {
		return religionCode;
	}

	public void setReligionCode(String religionCode) {
		this.religionCode = religionCode;
	}

	public String getTelephone() {
		return telephone;
	}

	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}

	public String getMobileTel() {
		return mobileTel;
	}

	public void setMobileTel(String mobileTel) {
		this.mobileTel = mobileTel;
	}

	public Long getAddressId() {
		return addressId;
	}

	public void setAddressId(Long addressId) {
		this.addressId = addressId;
	}

	public Long getJobCateId() {
		return jobCateId;
	}

	public void setJobCateId(Long jobCateId) {
		this.jobCateId = jobCateId;
	}

	public Long getEmsVersion() {
		return emsVersion;
	}

	public void setEmsVersion(Long emsVersion) {
		this.emsVersion = emsVersion;
	}

	public Long getInsertedby() {
		return insertedby;
	}

	public void setInsertedby(Long insertedby) {
		this.insertedby = insertedby;
	}

	public Long getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(Long updatedBy) {
		this.updatedBy = updatedBy;
	}

	public Date getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(Date insertTime) {
		this.insertTime = insertTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public Date getInsertTimestamp() {
		return insertTimestamp;
	}

	public void setInsertTimestamp(Date insertTimestamp) {
		this.insertTimestamp = insertTimestamp;
	}

	public Date getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Date updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public String getPartyType() {
		return partyType;
	}

	public void setPartyType(String partyType) {
		this.partyType = partyType;
	}

	public String getFatcaIndi() {
		return fatcaIndi;
	}

	public void setFatcaIndi(String fatcaIndi) {
		this.fatcaIndi = fatcaIndi;
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

	public Integer getCertiType() {
		return certiType;
	}

	public void setCertiType(Integer certiType) {
		this.certiType = certiType;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getOfficeTelReg() {
		return officeTelReg;
	}

	public void setOfficeTelReg(String officeTelReg) {
		this.officeTelReg = officeTelReg;
	}

	public String getOfficeTel() {
		return officeTel;
	}

	public void setOfficeTel(String officeTel) {
		this.officeTel = officeTel;
	}

	public String getOfficeTelExt() {
		return officeTelExt;
	}

	public void setOfficeTelExt(String officeTelExt) {
		this.officeTelExt = officeTelExt;
	}

	public String getHomeTelReg() {
		return homeTelReg;
	}

	public void setHomeTelReg(String homeTelReg) {
		this.homeTelReg = homeTelReg;
	}

	public String getHomeTel() {
		return homeTel;
	}

	public void setHomeTel(String homeTel) {
		this.homeTel = homeTel;
	}

	public String getHomeTelExt() {
		return homeTelExt;
	}

	public void setHomeTelExt(String homeTelExt) {
		this.homeTelExt = homeTelExt;
	}

	public BigDecimal getHeight() {
		return height;
	}

	public void setHeight(BigDecimal height) {
		this.height = height;
	}

	public BigDecimal getWeight() {
		return weight;
	}

	public void setWeight(BigDecimal weight) {
		this.weight = weight;
	}

	public Integer getBmi() {
		return bmi;
	}

	public void setBmi(Integer bmi) {
		this.bmi = bmi;
	}

	public String getNotifyIndi() {
		return notifyIndi;
	}

	public void setNotifyIndi(String notifyIndi) {
		this.notifyIndi = notifyIndi;
	}

	public Integer getPregnancyWeek() {
		return pregnancyWeek;
	}

	public void setPregnancyWeek(Integer pregnancyWeek) {
		this.pregnancyWeek = pregnancyWeek;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getMobileTel2() {
		return mobileTel2;
	}

	public void setMobileTel2(String mobileTel2) {
		this.mobileTel2 = mobileTel2;
	}

	public String getLeaderName() {
		return leaderName;
	}

	public void setLeaderName(String leaderName) {
		this.leaderName = leaderName;
	}

	public Long getPolicyCustId() {
		return policyCustId;
	}

	public void setPolicyCustId(Long policyCustId) {
		this.policyCustId = policyCustId;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public String getAuthorizationNo() {
		return authorizationNo;
	}

	public void setAuthorizationNo(String authorizationNo) {
		this.authorizationNo = authorizationNo;
	}

	public String getOfficeTelNational() {
		return officeTelNational;
	}

	public void setOfficeTelNational(String officeTelNational) {
		this.officeTelNational = officeTelNational;
	}

	public String getHomeTelNational() {
		return homeTelNational;
	}

	public void setHomeTelNational(String homeTelNational) {
		this.homeTelNational = homeTelNational;
	}

	public String getMobileTelNational() {
		return mobileTelNational;
	}

	public void setMobileTelNational(String mobileTelNational) {
		this.mobileTelNational = mobileTelNational;
	}

	public String getMobileTel2National() {
		return mobileTel2National;
	}

	public void setMobileTel2National(String mobileTel2National) {
		this.mobileTel2National = mobileTel2National;
	}

	public String getAccEnglishName() {
		return accEnglishName;
	}

	public void setAccEnglishName(String accEnglishName) {
		this.accEnglishName = accEnglishName;
	}

	public String getEnglishBankName() {
		return englishBankName;
	}

	public void setEnglishBankName(String englishBankName) {
		this.englishBankName = englishBankName;
	}

	public String getOiuBankAccount() {
		return oiuBankAccount;
	}

	public void setOiuBankAccount(String oiuBankAccount) {
		this.oiuBankAccount = oiuBankAccount;
	}

	public Long getOiubankId() {
		return oiubankId;
	}

	public void setOiubankId(Long oiubankId) {
		this.oiubankId = oiubankId;
	}

	public Long getCompanyCategory() {
		return companyCategory;
	}

	public void setCompanyCategory(Long companyCategory) {
		this.companyCategory = companyCategory;
	}

	public Date getEntryDate() {
		return entryDate;
	}

	public void setEntryDate(Date entryDate) {
		this.entryDate = entryDate;
	}

	public String getBankCode() {
		return bankCode;
	}

	public void setBankCode(String bankCode) {
		this.bankCode = bankCode;
	}

	public String getBranchCode() {
		return branchCode;
	}

	public void setBranchCode(String branchCode) {
		this.branchCode = branchCode;
	}

	public String getMarriageId() {
		return marriageId;
	}

	public void setMarriageId(String marriageId) {
		this.marriageId = marriageId;
	}

	public String getNationality2() {
		return nationality2;
	}

	public void setNationality2(String nationality2) {
		this.nationality2 = nationality2;
	}

	public String getEnglishName() {
		return englishName;
	}

	public void setEnglishName(String englishName) {
		this.englishName = englishName;
	}

	public String getRomanName() {
		return romanName;
	}

	public void setRomanName(String romanName) {
		this.romanName = romanName;
	}

	public String getAbandonInheritance() {
		return abandonInheritance;
	}

	public void setAbandonInheritance(String abandonInheritance) {
		this.abandonInheritance = abandonInheritance;
	}

	public String getCounterServices() {
		return counterServices;
	}

	public void setCounterServices(String counterServices) {
		this.counterServices = counterServices;
	}

	public String getLeaderRomanName() {
		return leaderRomanName;
	}

	public void setLeaderRomanName(String leaderRomanName) {
		this.leaderRomanName = leaderRomanName;
	}

	public String getGuardianAncmnt() {
		return guardianAncmnt;
	}

	public void setGuardianAncmnt(String guardianAncmnt) {
		this.guardianAncmnt = guardianAncmnt;
	}

	public String getOldForeignerId() {
		return oldForeignerId;
	}

	public void setOldForeignerId(String oldForeignerId) {
		this.oldForeignerId = oldForeignerId;
	}
	
	
}
