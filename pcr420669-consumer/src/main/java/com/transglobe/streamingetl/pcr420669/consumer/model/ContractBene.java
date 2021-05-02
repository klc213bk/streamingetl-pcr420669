package com.transglobe.streamingetl.pcr420669.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ContractBene {

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
	
	
}
