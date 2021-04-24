package com.transglobe.streamingetl.pcr420669.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.dataprep.DataPrep;


public class DataPrepareApp {
	private static final Logger logger = LoggerFactory.getLogger(DataPrepareApp.class);

	public static void main(String[] args){
		
	
		DataPrep dataPrep = null;
		try {
			dataPrep = new DataPrep("config.properties");

			logger.info(">>> DataPrep t_policy_holder !!!");
			dataPrep.truncateSourceTable("PMUSER", "T_POLICY_HOLDER");
			String policyHolderSqlFile = "./data/t_policy_holder.sql";
			dataPrep.insertDataIntoSource(policyHolderSqlFile, 1, null);
			
			logger.info(">>> DataPrep t_insured_list !!!");
			dataPrep.truncateSourceTable("PMUSER", "T_INSURED_LIST");
			String insuredListSqlFile = "./data/t_insured_list.sql";
			dataPrep.insertDataIntoSource(insuredListSqlFile, 1, null);
			
			logger.info(">>> DataPrep t_contract_bene !!!");
			dataPrep.truncateSourceTable("PMUSER", "T_CONTRACT_BENE");
			String contractBeneSqlFile = "./data/t_contract_bene.sql";
			dataPrep.insertDataIntoSource(contractBeneSqlFile, 1, null);
		
			
			dataPrep.truncateSourceTable("PMUSER", "T_ADDRESS");
			
			logger.info(">>> DataPrep t_address_4_policy_holder.sql !!!");
			String addressPolicyHolder = "./data/t_address_4_policy_holder.sql";
			dataPrep.insertDataIntoSource(addressPolicyHolder, 1, null);
			
			logger.info(">>> DataPrep t_address_4_insured_list.sql !!!");
			String addressInsuredList = "./data/t_address_4_insured_list.sql";
			dataPrep.insertDataIntoSource(addressInsuredList, 1, null);
			
			logger.info(">>> DataPrep t_address_4_contract_bene.sql !!!");
			String addressContractBene = "./data/t_address_4_contract_bene.sql";
			dataPrep.insertDataIntoSource(addressContractBene, 1, null);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
