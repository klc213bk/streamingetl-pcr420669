package com.transglobe.streamingetl.pcr420669.load;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.pcr420669.load.model.InterestedPartyContact;

public class InitialLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	static class LoadBean {
		String fullTableName;
		Integer roleType;
		Long startSeq;
		Long endSeq;
	}
	private Config config;

	public InitialLoadApp(String fileName) throws Exception {
		config = Config.getConfig(CONFIG_FILE_NAME);
	}
	public static void main(String[] args) {
		logger.info(">>> start run InitialLoadApp");

		long t0 = 0L;
		try {

			InitialLoadApp app = new InitialLoadApp(CONFIG_FILE_NAME);

			//			t0 = System.currentTimeMillis();
			//			app.test1();
			//			logger.info(" test1 span={}", (System.currentTimeMillis() - t0));
			//
			t0 = System.currentTimeMillis();
			app.test4();
			logger.info(" test3 span={}", (System.currentTimeMillis() - t0));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void test2() throws Exception {
		logger.info(">>> run test2");
		ExecutorService executor = Executors.newFixedThreadPool(3);
		try {

			List<String> fulleSourceTableNames = new ArrayList<>();
			fulleSourceTableNames.add(config.sourceTablePolicyHolder);
			//			fulleSourceTableNames.add(config.sourceTableInsuredList);
			//			fulleSourceTableNames.add(config.sourceTableContractBene);

			Map<String, Integer> roleTypeMap = new HashMap<>();
			roleTypeMap.put(config.sourceTablePolicyHolder, 1);
			roleTypeMap.put(config.sourceTableInsuredList, 2);
			roleTypeMap.put(config.sourceTableContractBene, 3);

			List<CompletableFuture<Map<String, String>>> futures = 
					fulleSourceTableNames.stream().map(t -> CompletableFuture.supplyAsync(
							() -> loadInterestedPartyContact(t, roleTypeMap.get(t)), executor))
					.collect(Collectors.toList());

			List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			for (Map<String, String> map : result) {
				String sourceTable = map.get("SOURCE_TABLE");
				String sinkTable = map.get("SINK_TABLE");
				String returnCode = map.get("RETURN_CODE");
				String recordCount = "";
				String errormsg = "";
				String stackTrace = "";
				if ("0".equals(returnCode)) {
					recordCount = map.get("RECORD_COUNT");
				} else {
					errormsg = map.get("ERROR_MSG");
					stackTrace = map.get("STACE_TRACE");
				}
				logger.info("sourceTable={}, sinkTable={}, returnCode={}, recordCount={}, errormsg={},stackTrace={}", 
						sourceTable, sinkTable, returnCode, recordCount, errormsg, stackTrace);
			}

		} finally {
			if (executor != null) executor.shutdown();
		}

	}

	private Map<String, String> loadInterestedPartyContact(String sourceTableName, Integer roleType){
		logger.info(">>> run loadInterestedPartyContact, table={}, roleType={}", sourceTableName, roleType);
		Connection sourceConn = null;
		Connection sinkConn = null;
		List<InterestedPartyContact> contactList = new ArrayList<>();
		Map<String, String> map = new HashMap<>();
		try {
			Class.forName(config.sourceDbDriver);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);

			String sql = "select " + roleType + " as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + sourceTableName + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id fetch next 10000 rows only";

			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet = stmt.executeQuery(sql);

			while (resultSet.next()) {
				InterestedPartyContact contact = new InterestedPartyContact();
				contact.setRoleType(resultSet.getInt("ROLE_TYPE"));
				contact.setListId(resultSet.getLong("LIST_ID"));
				contact.setPolicyId(resultSet.getLong("POLICY_ID"));
				contact.setName(resultSet.getString("NAME"));
				contact.setCertiCode(resultSet.getString("CERTI_CODE"));
				contact.setMobileTel(resultSet.getString("MOBILE_TEL"));
				contact.setEmail(resultSet.getString("EMAIL"));
				contact.setAddressId(resultSet.getLong("ADDRESS_ID"));
				contact.setAddress1(resultSet.getString("ADDRESS_1"));

				contactList.add(contact);

			}
			resultSet.close();
			stmt.close();

			logger.info("query completed for table {}" + sourceTableName);

			// save to sink db
			int count = 0;
			sinkConn = DriverManager.getConnection(config.sinkDbUrl, config.sinkDbUsername, config.sinkDbPassword);
			sinkConn.setAutoCommit(false); 
			PreparedStatement pstmt = sinkConn.prepareStatement(
					"insert into " + config.sinkTableParty + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
							+ " values (?,?,?,?,?,?,?,?,?)");	
			for (InterestedPartyContact contact : contactList) {
				count++;
				pstmt.setInt(1, contact.getRoleType());
				if (contact.getListId() == null) {
					pstmt.setNull(2, Types.BIGINT);
				} else {
					pstmt.setLong(2, contact.getListId());
				}
				if (contact.getPolicyId() == null) {
					pstmt.setNull(3, Types.BIGINT);
				} else {
					pstmt.setLong(3, contact.getPolicyId());
				}
				if (contact.getName() == null) {
					pstmt.setNull(4, Types.VARCHAR);
				} else {
					pstmt.setString(4, contact.getName());
				}
				if (contact.getCertiCode() == null) {
					pstmt.setNull(5, Types.VARCHAR);
				} else {
					pstmt.setString(5, contact.getCertiCode());
				}
				if (contact.getMobileTel() == null) {
					pstmt.setNull(6, Types.VARCHAR);
				} else {
					pstmt.setString(6, contact.getMobileTel());
				}
				if (contact.getEmail() == null) {
					pstmt.setNull(7, Types.VARCHAR);
				} else {
					pstmt.setString(7, contact.getEmail());
				}
				if (contact.getAddressId() == null) {
					pstmt.setNull(8, Types.BIGINT);
				} else {
					pstmt.setLong(8, contact.getAddressId());
				}
				if (contact.getAddress1() == null) {
					pstmt.setNull(9, Types.VARCHAR);
				} else {
					pstmt.setString(9, contact.getAddress1());
				}

				pstmt.addBatch();

				if (count % 500 == 0 || count == contactList.size()) {
					pstmt.executeBatch();//executing the batch  

					logger.info("   >>>count={}, execute batch", count);
				}
			}
			if (pstmt != null) pstmt.close();

			sinkConn.commit(); 

			map.put("RETURN_CODE", "0");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTableParty);
			map.put("RECORD_COUNT", String.valueOf(contactList.size()));

		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			map.put("RETURN_CODE", "-999");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTableParty);
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
					map.put("RETURN_CODE", "-999");
					map.put("ERROR_MSG", e.getMessage());
					map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
				}
			}
		}
		return map;
	}
	private void test1() throws Exception {
		Connection sourceConn = null;
		Connection sinkConn = null;
		try {
			Class.forName(config.sourceDbDriver);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);

			// truncate table
			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet = stmt.executeQuery(
					"select 1 as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from t_policy_holder a inner join t_address b on a.address_id = b.address_id \n" + 
							" union \n" + 
							" select 2 as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from t_insured_list a inner join t_address b on a.address_id = b.address_id \n" + 
							" union \n" + 
					"  select 3 as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from t_contract_bene a inner join t_address b on a.address_id = b.address_id \n" );


			List<InterestedPartyContact> contactList = new ArrayList<>();
			while (resultSet.next()) {
				InterestedPartyContact contact = new InterestedPartyContact();
				contact.setRoleType(resultSet.getInt("ROLE_TYPE"));
				contact.setListId(resultSet.getLong("LIST_ID"));
				contact.setPolicyId(resultSet.getLong("POLICY_ID"));
				contact.setName(resultSet.getString("NAME"));
				contact.setCertiCode(resultSet.getString("CERTI_CODE"));
				contact.setMobileTel(resultSet.getString("MOBILE_TEL"));
				contact.setEmail(resultSet.getString("EMAIL"));
				contact.setAddressId(resultSet.getLong("ADDRESS_ID"));
				contact.setAddress1(resultSet.getString("ADDRESS_1"));

				contactList.add(contact);

			}
			resultSet.close();
			stmt.close();

			// save to sink db
			int count = 0;
			sinkConn = DriverManager.getConnection(config.sinkDbUrl, config.sinkDbUsername, config.sinkDbPassword);
			sinkConn.setAutoCommit(false); 
			PreparedStatement pstmt = sinkConn.prepareStatement(
					"insert into " + config.sinkTableParty + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
							+ " values (?,?,?,?,?,?,?,?,?)");	
			for (InterestedPartyContact contact : contactList) {
				count++;
				pstmt.setInt(1, contact.getRoleType());
				if (contact.getListId() == null) {
					pstmt.setNull(2, Types.BIGINT);
				} else {
					pstmt.setLong(2, contact.getListId());
				}
				if (contact.getPolicyId() == null) {
					pstmt.setNull(3, Types.BIGINT);
				} else {
					pstmt.setLong(3, contact.getPolicyId());
				}
				if (contact.getName() == null) {
					pstmt.setNull(4, Types.VARCHAR);
				} else {
					pstmt.setString(4, contact.getName());
				}
				if (contact.getCertiCode() == null) {
					pstmt.setNull(5, Types.VARCHAR);
				} else {
					pstmt.setString(5, contact.getCertiCode());
				}
				if (contact.getMobileTel() == null) {
					pstmt.setNull(6, Types.VARCHAR);
				} else {
					pstmt.setString(6, contact.getMobileTel());
				}
				if (contact.getEmail() == null) {
					pstmt.setNull(7, Types.VARCHAR);
				} else {
					pstmt.setString(7, contact.getEmail());
				}
				if (contact.getAddressId() == null) {
					pstmt.setNull(8, Types.BIGINT);
				} else {
					pstmt.setLong(8, contact.getAddressId());
				}
				if (contact.getAddress1() == null) {
					pstmt.setNull(9, Types.VARCHAR);
				} else {
					pstmt.setString(9, contact.getAddress1());
				}

				pstmt.addBatch();

				if (count % 500 == 0 || count == contactList.size()) {
					pstmt.executeBatch();//executing the batch  

					logger.info("   >>>count={}, execute batch", count);
				}
			}
			if (pstmt != null) pstmt.close();

			sinkConn.commit(); 

		}  catch (Exception e) {
			throw e;
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					throw e;
				}
			}
		}
	}
	private void test3() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(11);
		try {

			Long maxSeq = 8935503221L;
		//	Long seq = 10000000L;
			Long beginSeq = 0L;
			Long intervalSeq = 100000L;
			List<LoadBean> loadBeanList = new ArrayList<>();
			while (beginSeq <= maxSeq) {
				LoadBean loadBean = new LoadBean();
				loadBean.fullTableName = config.sourceTablePolicyHolder;
				loadBean.roleType = 1;
				loadBean.startSeq = beginSeq;
				loadBean.endSeq = beginSeq + intervalSeq;
				loadBeanList.add(loadBean);
				
				beginSeq = beginSeq + intervalSeq;
			}
			LoadBean loadBean = new LoadBean();
			loadBean.fullTableName = config.sourceTablePolicyHolder;
			loadBean.roleType = 1;
			loadBean.startSeq = beginSeq;
			loadBean.endSeq = maxSeq;
			loadBeanList.add(loadBean);

			//			fulleSourceTableNames.add(config.sourceTableInsuredList);
			//			fulleSourceTableNames.add(config.sourceTableContractBene);

//			Map<String, Integer> roleTypeMap = new HashMap<>();
//			roleTypeMap.put(config.sourceTablePolicyHolder, 1);
//			roleTypeMap.put(config.sourceTableInsuredList, 2);
//			roleTypeMap.put(config.sourceTableContractBene, 3);
			
			List<CompletableFuture<Map<String, String>>> futures = 
					loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
							() -> loadInterestedPartyContact3(t.fullTableName, t.roleType, t.startSeq, t.endSeq), executor))
					.collect(Collectors.toList());

			List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			for (Map<String, String> map : result) {
				String sourceTable = map.get("SOURCE_TABLE");
				String sinkTable = map.get("SINK_TABLE");
				String returnCode = map.get("RETURN_CODE");
				String recordCount = "";
				String errormsg = "";
				String stackTrace = "";
				if ("0".equals(returnCode)) {
					recordCount = map.get("RECORD_COUNT");
				} else {
					errormsg = map.get("ERROR_MSG");
					stackTrace = map.get("STACE_TRACE");
				}
				logger.info("sourceTable={}, sinkTable={}, returnCode={}, recordCount={}, errormsg={},stackTrace={}", 
						sourceTable, sinkTable, returnCode, recordCount, errormsg, stackTrace);
			}

		} finally {
			if (executor != null) executor.shutdown();
		}

	}
	private Map<String, String> loadInterestedPartyContact3(String sourceTableName, Integer roleType, Long startSeq, Long endSeq){
		logger.info(">>> run loadInterestedPartyContact, table={}, roleType={}", sourceTableName, roleType);
		Connection sourceConn = null;
		Connection sinkConn = null;
		List<InterestedPartyContact> contactList = new ArrayList<>();
		Map<String, String> map = new HashMap<>();
		try {
			Class.forName(config.sourceDbDriver);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);

			String sql = "select " + roleType + " as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + sourceTableName + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id "
					+ " where " + startSeq + " <= a.address_id and a.address_id < " + endSeq;

			logger.info(">>> sql= {}",sql);

			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet = stmt.executeQuery(sql);

			//			Long c = 0L;
			while (resultSet.next()) {
				//				c++;
				//				if ( c % 1000 == 0) {
				//					logger.info(">>> query count= {}",c);
				//				}
				InterestedPartyContact contact = new InterestedPartyContact();
				contact.setRoleType(resultSet.getInt("ROLE_TYPE"));
				contact.setListId(resultSet.getLong("LIST_ID"));
				contact.setPolicyId(resultSet.getLong("POLICY_ID"));
				contact.setName(resultSet.getString("NAME"));
				contact.setCertiCode(resultSet.getString("CERTI_CODE"));
				contact.setMobileTel(resultSet.getString("MOBILE_TEL"));
				contact.setEmail(resultSet.getString("EMAIL"));
				contact.setAddressId(resultSet.getLong("ADDRESS_ID"));
				contact.setAddress1(resultSet.getString("ADDRESS_1"));

				contactList.add(contact);

			}
			resultSet.close();
			stmt.close();

			logger.info("query completed for table {}",sourceTableName);

			// save to sink db
			int count = 0;
			sinkConn = DriverManager.getConnection(config.sinkDbUrl, config.sinkDbUsername, config.sinkDbPassword);
			sinkConn.setAutoCommit(false); 
			PreparedStatement pstmt = sinkConn.prepareStatement(
					"insert into " + config.sinkTableParty + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
							+ " values (?,?,?,?,?,?,?,?,?)");	
			for (InterestedPartyContact contact : contactList) {
				count++;
				pstmt.setInt(1, contact.getRoleType());
				if (contact.getListId() == null) {
					pstmt.setNull(2, Types.BIGINT);
				} else {
					pstmt.setLong(2, contact.getListId());
				}
				if (contact.getPolicyId() == null) {
					pstmt.setNull(3, Types.BIGINT);
				} else {
					pstmt.setLong(3, contact.getPolicyId());
				}
				if (contact.getName() == null) {
					pstmt.setNull(4, Types.VARCHAR);
				} else {
					pstmt.setString(4, contact.getName());
				}
				if (contact.getCertiCode() == null) {
					pstmt.setNull(5, Types.VARCHAR);
				} else {
					pstmt.setString(5, contact.getCertiCode());
				}
				if (contact.getMobileTel() == null) {
					pstmt.setNull(6, Types.VARCHAR);
				} else {
					pstmt.setString(6, contact.getMobileTel());
				}
				if (contact.getEmail() == null) {
					pstmt.setNull(7, Types.VARCHAR);
				} else {
					pstmt.setString(7, contact.getEmail());
				}
				if (contact.getAddressId() == null) {
					pstmt.setNull(8, Types.BIGINT);
				} else {
					pstmt.setLong(8, contact.getAddressId());
				}
				if (contact.getAddress1() == null) {
					pstmt.setNull(9, Types.VARCHAR);
				} else {
					pstmt.setString(9, contact.getAddress1());
				}

				pstmt.addBatch();

				if (count % 1000 == 0 || count == contactList.size()) {
					pstmt.executeBatch();//executing the batch  

					logger.info("   >>>count={}, execute batch", count);
				}
			}
			logger.info("   >>>count={}, startSeq={},endSeq={} ", count, startSeq, endSeq);
			
			if (pstmt != null) pstmt.close();

			sinkConn.commit(); 

			map.put("RETURN_CODE", "0");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTableParty);
			map.put("RECORD_COUNT", String.valueOf(contactList.size()));

		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			map.put("RETURN_CODE", "-999");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTableParty);
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
					map.put("RETURN_CODE", "-999");
					map.put("ERROR_MSG", e.getMessage());
					map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
				}
			}
		}
		return map;
	}
	private Map<String, String> loadInterestedPartyContact4(String sourceTableName, Integer roleType, Long startSeq, Long endSeq){
		logger.info(">>> run loadInterestedPartyContact, table={}, roleType={}", sourceTableName, roleType);
		Connection sourceConn = null;
		Connection sinkConn = null;

		Map<String, String> map = new HashMap<>();
		try {
			Class.forName(config.sourceDbDriver);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);
	
			String sql = "select " + roleType + " as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + sourceTableName + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id "
					+ " where " + startSeq + " <= a.address_id and a.address_id < " + endSeq;
	
			logger.info(">>> sql= {}",sql);
	
			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet = stmt.executeQuery(sql);
	
			sinkConn = DriverManager.getConnection(config.sinkDbUrl, config.sinkDbUsername, config.sinkDbPassword);
			sinkConn.setAutoCommit(false); 
			PreparedStatement pstmt = sinkConn.prepareStatement(
					"insert into " + config.sinkTableParty + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
							+ " values (?,?,?,?,?,?,?,?,?)");
			Long count = 0L;
			while (resultSet.next()) {
				count++;
				
				pstmt.setInt(1, resultSet.getInt("ROLE_TYPE"));
				Long listId = resultSet.getLong("LIST_ID");
				Long policyId = resultSet.getLong("POLICY_ID");
				String name = resultSet.getString("NAME");
				String certiCode = resultSet.getString("CERTI_CODE");
				String mobileTel = resultSet.getString("MOBILE_TEL");
				String email = resultSet.getString("EMAIL");
				Long addressId = resultSet.getLong("ADDRESS_ID");
				String address1 = resultSet.getString("ADDRESS_1");
				
				pstmt.setLong(2, listId);
				pstmt.setLong(3, policyId);
				
				if (name == null) {
					pstmt.setNull(4, Types.VARCHAR);
				} else {
					pstmt.setString(4, name);
				}
				if (certiCode== null) {
					pstmt.setNull(5, Types.VARCHAR);
				} else {
					pstmt.setString(5, certiCode);
				}
				if (mobileTel == null) {
					pstmt.setNull(6, Types.VARCHAR);
				} else {
					pstmt.setString(6, mobileTel);
				}
				if (email == null) {
					pstmt.setNull(7, Types.VARCHAR);
				} else {
					pstmt.setString(7, email);
				}
				pstmt.setLong(8, addressId);
				if (address1 == null) {
					pstmt.setNull(9, Types.VARCHAR);
				} else {
					pstmt.setString(9, address1);
				}
	
				pstmt.addBatch();
				
				if (count % 1000 == 0) {
					pstmt.executeBatch();//executing the batch  
	
					logger.info("   >>>count={}, execute batch", count);
				}
			}
			logger.info("   >>>count={}, startSeq={},endSeq={} ", count, startSeq, endSeq);
			pstmt.executeBatch();
			if (pstmt != null) pstmt.close();
			if (count > 0) sinkConn.commit(); 
			
			resultSet.close();
			stmt.close();

			map.put("RETURN_CODE", "0");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTableParty);
			map.put("RECORD_COUNT", String.valueOf(count));
	
		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			map.put("RETURN_CODE", "-999");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTableParty);
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
					map.put("RETURN_CODE", "-999");
					map.put("ERROR_MSG", e.getMessage());
					map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
				}
			}
		}
		return map;
	}
	private void test4() throws Exception {
	
			ExecutorService executor = Executors.newFixedThreadPool(11);
			try {
	
				Long maxSeq = 8935503221L;
			//	Long seq = 10000000L;
				Long beginSeq = 0L;
				Long intervalSeq = 100000L;
				List<LoadBean> loadBeanList = new ArrayList<>();
				while (beginSeq <= maxSeq) {
					LoadBean loadBean = new LoadBean();
					loadBean.fullTableName = config.sourceTablePolicyHolder;
					loadBean.roleType = 1;
					loadBean.startSeq = beginSeq;
					loadBean.endSeq = beginSeq + intervalSeq;
					loadBeanList.add(loadBean);
					
					beginSeq = beginSeq + intervalSeq;
				}
				LoadBean loadBean = new LoadBean();
				loadBean.fullTableName = config.sourceTablePolicyHolder;
				loadBean.roleType = 1;
				loadBean.startSeq = beginSeq;
				loadBean.endSeq = maxSeq;
				loadBeanList.add(loadBean);
	
				//			fulleSourceTableNames.add(config.sourceTableInsuredList);
				//			fulleSourceTableNames.add(config.sourceTableContractBene);
	
	//			Map<String, Integer> roleTypeMap = new HashMap<>();
	//			roleTypeMap.put(config.sourceTablePolicyHolder, 1);
	//			roleTypeMap.put(config.sourceTableInsuredList, 2);
	//			roleTypeMap.put(config.sourceTableContractBene, 3);
				
				List<CompletableFuture<Map<String, String>>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> loadInterestedPartyContact4(t.fullTableName, t.roleType, t.startSeq, t.endSeq), executor))
						.collect(Collectors.toList());
	
				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
	
				for (Map<String, String> map : result) {
					String sourceTable = map.get("SOURCE_TABLE");
					String sinkTable = map.get("SINK_TABLE");
					String returnCode = map.get("RETURN_CODE");
					String recordCount = "";
					String errormsg = "";
					String stackTrace = "";
					if ("0".equals(returnCode)) {
						recordCount = map.get("RECORD_COUNT");
					} else {
						errormsg = map.get("ERROR_MSG");
						stackTrace = map.get("STACE_TRACE");
					}
					logger.info("sourceTable={}, sinkTable={}, returnCode={}, recordCount={}, errormsg={},stackTrace={}", 
							sourceTable, sinkTable, returnCode, recordCount, errormsg, stackTrace);
				}
	
			} finally {
				if (executor != null) executor.shutdown();
			}
	
		}
}
