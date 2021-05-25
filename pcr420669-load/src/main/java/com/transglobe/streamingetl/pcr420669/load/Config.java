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
	public String sourceTablePolicyHolder;
	public String sourceTableInsuredList;
	public String sourceTableContractBene;
	public String sourceTableAddress;
	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkTablePartyContact;
	public String sinkTablePartyContactTemp;
	public String sinkTableStreamingEtlHealth;


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
			dbConfig.sourceTablePolicyHolder = prop.getProperty("source.table.policy_holder");
			dbConfig.sourceTableInsuredList = prop.getProperty("source.table.insured_list");
			dbConfig.sourceTableContractBene = prop.getProperty("source.table.contract_bene");
			dbConfig.sourceTableAddress = prop.getProperty("source.table.address");

			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			dbConfig.sinkTablePartyContact = prop.getProperty("sink.table.party_contact");
			dbConfig.sinkTablePartyContactTemp = prop.getProperty("sink.table.party_contact_temp");
			dbConfig.sinkTableStreamingEtlHealth = prop.getProperty("sink.table.streaming_etl_health");

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
