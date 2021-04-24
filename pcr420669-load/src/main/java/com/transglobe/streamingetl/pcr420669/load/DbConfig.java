package com.transglobe.streamingetl.pcr420669.load;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class DbConfig {
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;

	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkDbUsername;
	public String sinkDbPassword;

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
