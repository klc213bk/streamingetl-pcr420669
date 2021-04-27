package com.transglobe.streamingetl.pcr420669.consumer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {

	public String sourceTopicPolicyHolder;
	public String sourceTopicInsuredList;
	public String sourceTopicContractBene;
	public String sourceTopicAddress;
	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkDbUsername;
	public String sinkDbPassword;
	public String sinkTableParty;
	public String bootstrapServers;
	public String groupId;
	public List<String> topicList;

	public static Config getConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);


			Config config = new Config();
			config.sourceTopicPolicyHolder = prop.getProperty("source.topic.policyholder");
			config.sourceTopicInsuredList = prop.getProperty("source.Topic.insuredlist");
			config.sourceTopicContractBene = prop.getProperty("source.topic.contractbene");
			config.sourceTopicAddress = prop.getProperty("source.table_address");

			config.sinkDbDriver = prop.getProperty("sink.db.driver");
			config.sinkDbUrl = prop.getProperty("sink.db.url");
			config.sinkDbUsername = prop.getProperty("sink.db.username");
			config.sinkDbPassword = prop.getProperty("sink.db.password");
			config.sinkTableParty = prop.getProperty("sink.table_party");
			
			config.bootstrapServers = prop.getProperty("bootstrap.servers");
			config.groupId = prop.getProperty("group.id");
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
