package com.transglobe.streamingetl.pcr420669.load;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	public String sinkDbDriver;
	public String sinkDbUrl;


	public static Config getConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);


			Config dbConfig = new Config();
			dbConfig.sourceDbDriver = prop.getProperty("source.db.driver");
			dbConfig.sourceDbUrl = prop.getProperty("source.db.url");
			dbConfig.sourceDbUsername = prop.getProperty("source.db.username");
			dbConfig.sourceDbPassword = prop.getProperty("source.db.password");

			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");

			return dbConfig;
		} catch (Exception e) {
			throw e;
		} 
	}
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
