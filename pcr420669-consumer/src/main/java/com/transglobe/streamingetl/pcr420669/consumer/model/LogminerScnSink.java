package com.transglobe.streamingetl.pcr420669.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LogminerScnSink {

	private Long id;
	
	@JsonProperty("STREAMING_NAME")
	private String streamingName;
	
	@JsonProperty("SCN")
	private Long scn;
	
	@JsonProperty("SCN_INSERT_TIME")
	private Long scnInsertTime;
	
	@JsonProperty("SCN_UPDATE_TIME")
	private Long scnUpdateTime;
	
	@JsonProperty("HEALTH_TIME")
	private Long healthTime;

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

	public Long getScnInsertTime() {
		return scnInsertTime;
	}

	public void setScnInsertTime(Long scnInsertTime) {
		this.scnInsertTime = scnInsertTime;
	}

	public Long getScnUpdateTime() {
		return scnUpdateTime;
	}

	public void setScnUpdateTime(Long scnUpdateTime) {
		this.scnUpdateTime = scnUpdateTime;
	}

	public Long getHealthTime() {
		return healthTime;
	}

	public void setHealthTime(Long healthTime) {
		this.healthTime = healthTime;
	}
	
	
}
