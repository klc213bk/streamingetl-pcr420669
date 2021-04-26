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
	public String sinkDbUsername;
	public String sinkDbPassword;
	public String sinkTableParty;

//	public List<String> getSourceTableOwners() {
//
//		List<String> ownerList = new ArrayList<>();
//		for (String table : sourceTables) {
//			ownerList.add(table.split("\\.")[0]);
//		}
//		return ownerList;
//	}
//	public List<String> getSourceTableNames() {
//
//		List<String> tableNameList = new ArrayList<>();
//		for (String table : sourceTables) {
//			tableNameList.add(table.split("\\.")[1]);
//		}
//		return tableNameList;
//	}
//	public List<String> getSourceFullTableNames() {
//
//		List<String> fullTablenames = new ArrayList<>();
//		List<String> tableOwners = getSourceTableOwners();
//		List<String> tableNames = getSourceTableNames();
//		for (int i = 0; i < tableOwners.size(); i++) {
//			fullTablenames.add(tableOwners.get(i) + "." + tableNames.get(i));
//		}
//		
//		return fullTablenames;
//	}
//	public List<String> getSinkTableOwners() {
//
//		List<String> ownerList = new ArrayList<>();
//		for (String table : sinkTables) {
//			ownerList.add(table.split("\\.")[0]);
//		}
//		return ownerList;
//	}
//	public List<String> getSinkTableNames() {
//
//		List<String> tableNameList = new ArrayList<>();
//		for (String table : sinkTables) {
//			tableNameList.add(table.split("\\.")[1]);
//		}
//		return tableNameList;
//	}
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
			dbConfig.sourceTablePolicyHolder = prop.getProperty("source.table_policyholder");
			dbConfig.sourceTableInsuredList = prop.getProperty("source.table_insuredlist");
			dbConfig.sourceTableContractBene = prop.getProperty("source.table_contractbene");
			dbConfig.sourceTableAddress = prop.getProperty("source.table_address");

			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			dbConfig.sinkDbUsername = prop.getProperty("sink.db.username");
			dbConfig.sinkDbPassword = prop.getProperty("sink.db.password");
			dbConfig.sinkTableParty = prop.getProperty("sink.table_party");

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
