package com.transglobe.streamingetl.pcr420669.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Address {

	@JsonProperty("ADDRESS_ID")
	private Long addressId;
	
	@JsonProperty("ADDRESS_1")
	private String address1;

	public Long getAddressId() {
		return addressId;
	}

	public void setAddressId(Long addressId) {
		this.addressId = addressId;
	}

	public String getAddress1() {
		return address1;
	}

	public void setAddress1(String address1) {
		this.address1 = address1;
	}
	
	
}
