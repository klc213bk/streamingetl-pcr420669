package com.transglobe.streamingetl.pcr420669.consumer.model;

import java.sql.Timestamp;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LogminerScnSink {

	private Long id;
	
	@JsonProperty("STREAMING_NAME")
	private String streamingName;
	
	@JsonProperty("SCN")
	private Long scn;
	
	@JsonProperty("SCN_INSERT_TIME")
	private Timestamp scnInsertTime;
	
	@JsonProperty("SCN_UPDATE_TIME")
	private Timestamp scnUpdateTime;
	
	@JsonProperty("HEALTH_TIME")
	private Timestamp healthTime;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getStreamingName() {
		return streamingName;
	}

	public void setStreamingName(String streamingName) {
		this.streamingName = streamingName;
	}

	public Long getScn() {
		return scn;
	}

	public void setScn(Long scn) {
		this.scn = scn;
	}

	public Timestamp getScnInsertTime() {
		return scnInsertTime;
	}

	public void setScnInsertTime(Timestamp scnInsertTime) {
		this.scnInsertTime = scnInsertTime;
	}

	public Timestamp getScnUpdateTime() {
		return scnUpdateTime;
	}

	public void setScnUpdateTime(Timestamp scnUpdateTime) {
		this.scnUpdateTime = scnUpdateTime;
	}

	public Timestamp getHealthTime() {
		return healthTime;
	}

	public void setHealthTime(Timestamp healthTime) {
		this.healthTime = healthTime;
	}
	
	
}
