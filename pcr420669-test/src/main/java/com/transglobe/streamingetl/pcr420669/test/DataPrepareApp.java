package com.transglobe.streamingetl.pcr420669.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.util.ConfigUtils;
import com.transglobe.streamingetl.common.util.DataUtils;

public class DataPrepareApp {
	private static final Logger logger = LoggerFactory.getLogger(DataPrepareApp.class);

	public static void main(String[] args){
		
	
	//	DataPrep dataPrep = null;
		try {
		//	dataPrep = new DataPrep("config.properties");
			
			Properties prop = ConfigUtils.getProperties("config.properties");
			String sourceDbDriver = prop.getProperty("source.db.driver");
			String sourceDbUrl = prop.getProperty("source.db.url");
			String sourceDbUsername = prop.getProperty("source.db.username");
			String sourceDbPassword = prop.getProperty("source.db.password");
					
			Class.forName(sourceDbDriver);
			Connection sourceConn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword);
			

			DataUtils.truncateTable(sourceConn, "PMUSER", "T_POLICY_HOLDER");
			DataUtils.truncateTable(sourceConn, "PMUSER", "T_INSURED_LIST");
			DataUtils.truncateTable(sourceConn, "PMUSER", "T_CONTRACT_BENE");
			DataUtils.truncateTable(sourceConn, "PMUSER", "T_ADDRESS");
			
			logger.info(">>> DataPrep t_policy_holder !!!");
			DataUtils.truncateTable(sourceConn, "PMUSER", "T_POLICY_HOLDER");
			String policyHolderSqlFile = "./data/t_policy_holder_data.sql";
			DataUtils.insertDataIntoSource(sourceConn, policyHolderSqlFile, 1, null);;
			String addressPolicyHolder = "./data/t_address_4_policy_holder_data.sql";
			DataUtils.insertDataIntoSource(sourceConn, addressPolicyHolder, 1, null);
			
			
			logger.info(">>> DataPrep t_insured_list !!!");
			String insuredListSqlFile = "./data/t_insured_list_data.sql";
			DataUtils.insertDataIntoSource(sourceConn, insuredListSqlFile, 1, null);
			String addressInsuredList = "./data/t_address_4_insured_list_data.sql";
			DataUtils.insertDataIntoSource(sourceConn, addressInsuredList, 1, null);
			
			logger.info(">>> DataPrep t_contract_bene !!!");		
			String contractBeneSqlFile = "./data/t_contract_bene_data.sql";
			DataUtils.insertDataIntoSource(sourceConn, contractBeneSqlFile, 1, null);
			String addressContractBene = "./data/t_address_4_contract_bene_data.sql";
			DataUtils.insertDataIntoSource(sourceConn, addressContractBene, 1, null);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
