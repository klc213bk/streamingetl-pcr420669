package com.transglobe.streamingetl.pcr420669.consumer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {

	public String sourceTablePolicyHolder;
	public String sourceTableInsuredList;
	public String sourceTableContractBene;
	public String sourceTablePolicyHolderLog;
	public String sourceTableInsuredListLog;
	public String sourceTableContractBeneLog;
	public String sourceTableAddress;
	public String sourceTableStreamingEtlHealthCdc;
	
	public String sourceSyncTableContractMaster;
	public String sourceSyncTablePolicyChange;
	
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	
	public String sinkDbDriver;
	public String sinkDbUrl;
//	public String sinkDbUsername;
//	public String sinkDbPassword;
	public String sinkTablePartyContact;
	public String sinkTablePartyContactTemp;
	public String sinkTableStreamingEtlHealth;
	public String bootstrapServers;
	public List<String> topicList;
	
	public static Config getConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);


			Config config = new Config();
			config.sourceTablePolicyHolder = prop.getProperty("source.table.policy_holder");
			config.sourceTableInsuredList = prop.getProperty("source.table.insured_list");
			config.sourceTableContractBene = prop.getProperty("source.table.contract_bene");
			config.sourceTablePolicyHolderLog = prop.getProperty("source.table.policy_holder_log");
			config.sourceTableInsuredListLog = prop.getProperty("source.table.insured_list_log");
			config.sourceTableContractBeneLog = prop.getProperty("source.table.contract_bene_log");
			
			config.sourceTableAddress = prop.getProperty("source.table.address");
			config.sourceTableStreamingEtlHealthCdc = prop.getProperty("source.table.streaming.etl.health.cdc");

			config.sourceSyncTableContractMaster = prop.getProperty("source.sync.table.contract_master");
			config.sourceSyncTablePolicyChange = prop.getProperty("source.sync.table.policy_change");
			
			config.sourceDbDriver = prop.getProperty("source.db.driver");
			config.sourceDbUrl = prop.getProperty("source.db.url");
			config.sourceDbUsername = prop.getProperty("source.db.username");
			config.sourceDbPassword = prop.getProperty("source.db.password");
					
			config.sinkDbDriver = prop.getProperty("sink.db.driver");
			config.sinkDbUrl = prop.getProperty("sink.db.url");
//			config.sinkDbUsername = prop.getProperty("sink.db.username");
//			config.sinkDbPassword = prop.getProperty("sink.db.password");
			config.sinkTablePartyContact = prop.getProperty("sink.table.party_contact");
			config.sinkTableStreamingEtlHealth=prop.getProperty("sink.table.streaming_etl_health");
					
			config.bootstrapServers = prop.getProperty("bootstrap.servers");
			String[] topicArr = prop.getProperty("topics").split(",");
			config.topicList = Arrays.asList(topicArr);
			
			return config;
		} catch (Exception e) {
			throw e;
		} 
	}
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
