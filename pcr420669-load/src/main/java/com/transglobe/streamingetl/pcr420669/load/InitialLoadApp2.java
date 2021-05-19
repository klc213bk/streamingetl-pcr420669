package com.transglobe.streamingetl.pcr420669.load;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InitialLoadApp [loaddata]
 *  dataload rule
 *  1. 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
 *  so, set email to null if role_type = 3
 *  
 * @author oracle
 *
 */
public class InitialLoadApp2 {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp2.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	private static final String CREATE_TABLE_FILE_NAME = "createtable-T_PARTY_CONTACT.sql";
	private static final String CREATE_TEMP_TABLE_FILE_NAME = "createtable-T_PARTY_CONTACT_TEMP.sql";

	private static final int THREADS = 20;

//	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;

	static class LoadBean {
		String fullTableName;
		Integer roleType;
		Long startSeq;
		Long endSeq;
	}
	private Config config;

	public InitialLoadApp2(String fileName) throws Exception {
		config = Config.getConfig(fileName);
		//	this.createTableFile = createTableFile;

		sourceConnectionPool = new BasicDataSource();

		sourceConnectionPool.setUrl(config.sourceDbUrl);
		sourceConnectionPool.setUsername(config.sourceDbUsername);
		sourceConnectionPool.setPassword(config.sourceDbPassword);
		sourceConnectionPool.setDriverClassName(config.sourceDbDriver);
		sourceConnectionPool.setMaxTotal(THREADS);

		sinkConnectionPool = new BasicDataSource();
		sinkConnectionPool.setUrl(config.sinkDbUrl);
		sinkConnectionPool.setDriverClassName(config.sinkDbDriver);
		sinkConnectionPool.setMaxTotal(THREADS);

	}
	private void close() {
		try {
			if (sourceConnectionPool != null) sourceConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		try {
			if (sinkConnectionPool != null) sinkConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
	public static void main(String[] args) {
		logger.info(">>> start run InitialLoadApp");

		boolean loaddata = false;
		if (args.length != 0 && StringUtils.equals("loaddata", args[0])) {
			loaddata = true;
		}


		Long t0 = System.currentTimeMillis();

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;
			String createTableFile = StringUtils.isBlank(profileActive)? CREATE_TABLE_FILE_NAME : profileActive + "/" + CREATE_TABLE_FILE_NAME;
			String createTempTableFile = StringUtils.isBlank(profileActive)? CREATE_TEMP_TABLE_FILE_NAME : profileActive + "/" + CREATE_TEMP_TABLE_FILE_NAME;

			InitialLoadApp2 app = new InitialLoadApp2(configFile);

			// create sink table
			logger.info(">>>  Start: dropTable, tableName={}", app.config.sinkTablePartyContact);
			app.dropTable(app.config.sinkTablePartyContact);
			app.dropTable(app.config.sinkTablePartyContactTemp);
			logger.info(">>>  End: dropTable DONE!!!");

			logger.info(">>>  Start: createTable, tableName={}, createTableFile={}", app.config.sinkTablePartyContact, createTableFile);			
			app.createTable(app.config.sinkTablePartyContact, createTableFile);
			app.createTable(app.config.sinkTablePartyContactTemp, createTempTableFile);
			logger.info(">>>  End: createTable DONE!!!");

			logger.info("init tables span={}, ", (System.currentTimeMillis() - t0));						

			if (loaddata) {
				app.run();
			}
			logger.info("run load data span={}, ", (System.currentTimeMillis() - t0));

			// create indexes
			app.runCreateIndexes();


			app.close();


			logger.info("total load span={}, ", (System.currentTimeMillis() - t0));


			System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}



	private Map<String, String> loadPartyContact(String sourceTableName, Integer roleType, Long startSeq, Long endSeq){
		//		logger.info(">>> run loadInterestedPartyContact, table={}, roleType={}", sourceTableName, roleType);

		Console cnsl = null;
		Map<String, String> map = new HashMap<>();
		try {

			Connection sourceConn = this.sourceConnectionPool.getConnection();
			Connection sinkConn = this.sinkConnectionPool.getConnection();

			String sql = "select a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + sourceTableName + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id "
					+ " where " + startSeq + " <= a.list_id and a.list_id < " + endSeq ;
			//		+ " and rownum < 10";
			//	+ " fetch next 10 rows only";

			//		logger.info(">>> sql= {}", sql);

			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet = stmt.executeQuery(sql);

			sinkConn.setAutoCommit(false); 
			PreparedStatement pstmt = sinkConn.prepareStatement(
					"insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
							+ " values (?,?,?,?,?,?,?,?,?)");

			Long count = 0L;
			while (resultSet.next()) {
				count++;

				Long listId = resultSet.getLong("LIST_ID");
				Long policyId = resultSet.getLong("POLICY_ID");
				String name = resultSet.getString("NAME");
				String certiCode = resultSet.getString("CERTI_CODE");
				String mobileTel = resultSet.getString("MOBILE_TEL");
				//因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
				String email = (roleType == 3)? null : resultSet.getString("EMAIL");
				Long addressId = resultSet.getLong("ADDRESS_ID");
				String address1 = resultSet.getString("ADDRESS_1");

				pstmt.setInt(1, roleType);
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

				if (count % 3000 == 0) {
					pstmt.executeBatch();//executing the batch  
					sinkConn.commit(); 
					pstmt.clearBatch();
				}
			}
		//	if (startSeq % 50000000 == 0) {
				//				
				cnsl = System.console();
				//				logger.info("   >>>roletype={}, startSeq={}, count={}, span={} ", roleType, startSeq, count, (System.currentTimeMillis() - t0));
				cnsl.printf("   >>>roletype=%d, startSeq=%d, endSeq=%d, count=%d \n", roleType, startSeq, endSeq, count);

				//				cnsl.printf("   >>>roletype=" + roleType + ", startSeq=" + startSeq + ", count=" + count +", span=" + ",span=" + (System.currentTimeMillis() - t0));
				cnsl.flush();
	//		}

			pstmt.executeBatch();
			if (pstmt != null) pstmt.close();
			if (count > 0) sinkConn.commit(); 

			resultSet.close();
			stmt.close();

			sourceConn.close();
			sinkConn.close();

			//			map.put("RETURN_CODE", "0");
			//			map.put("SOURCE_TABLE", sourceTableName);
			//			map.put("SINK_TABLE", config.sinkTablePartyContact);
			//			map.put("RECORD_COUNT", String.valueOf(count));

		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			map.put("RETURN_CODE", "-999");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTablePartyContact);
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
		} finally {

		}
		return map;
	}
	private void run() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {

			Connection sourceConn = this.sourceConnectionPool.getConnection();

			String table = null;
			Integer roleType = null;
			for (int i = 0; i < 3; i++) {
				if ( i == 0) {
					table = config.sourceTablePolicyHolder;
					roleType = 1;
				} else if ( i == 1) {
					table = config.sourceTableInsuredList;
					roleType = 2;
				} else if ( i == 2) {
					table = config.sourceTableContractBene;
					roleType = 3;
				}
				sql = "select max(list_id) as MAX_LIST_ID from " + table;
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				long maxListId = 0;
				while (rs.next()) {
					maxListId = rs.getLong("MAX_LIST_ID");
				}
				rs.close();
				pstmt.close();

				long stepSize = 10000;
				long startIndex = 0;
				
				int totalPartyCount = 0;
				List<LoadBean> loadBeanList = new ArrayList<>();
				while (startIndex <= maxListId) {
					long endIndex = startIndex + stepSize;

					/*
					sql = "select count(*) as party_count from " + table + " a inner join t_address b on a.address_id = b.address_id "
							+ " where a.address_id >= ? and a.address_id < ?";
					pstmt = sourceConn.prepareStatement(sql);
					pstmt.setLong(1, startIndex);
					pstmt.setLong(2, endIndex);
					rs = pstmt.executeQuery();
					int partyCount = 0;
					while (rs.next()) {
						partyCount = rs.getInt("PARTY_COUNT");
					}	
					rs.close();
					pstmt.close();

					//	logger.info("partyCount={}", partyCount);
					totalPartyCount +=  partyCount;

*/
//					if (partyCount > 0) {
//						int div = 10;
//						long subStepSize = stepSize / div;
//						for (int j = 0; j < div; j++) {
					int j = 0;
					long  subStepSize = stepSize;
							LoadBean loadBean = new LoadBean();
							loadBean.fullTableName = table;
							loadBean.roleType = roleType;
							loadBean.startSeq = startIndex + j * subStepSize;
							loadBean.endSeq = startIndex + (j + 1) * subStepSize;
							loadBeanList.add(loadBean);
//
//						}
//					}

					startIndex = endIndex;
				}

				logger.info("table={}, maxlistid={}, size={}, total partyCount={}", table, maxListId, loadBeanList.size(), totalPartyCount);

				List<CompletableFuture<Map<String, String>>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> loadPartyContact(t.fullTableName, t.roleType, t.startSeq, t.endSeq), executor))
						.collect(Collectors.toList());			



				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());




			}
			/*
			List<String> fullTableNameList = new ArrayList<>();
			fullTableNameList.add(config.sourceTablePolicyHolder);
			fullTableNameList.add(config.sourceTableInsuredList);
			fullTableNameList.add(config.sourceTableContractBene);
			for (String fullTableName : fullTableNameList) {
				Long beginSeq = 0L;

				List<LoadBean> loadBeanList = new ArrayList<>();
				while (beginSeq <= maxSeq) {
					LoadBean loadBean = new LoadBean();
					loadBean.fullTableName = fullTableName;
					if (config.sourceTablePolicyHolder.equals(fullTableName)) {
						loadBean.roleType = 1;
					} else if (config.sourceTableInsuredList.equals(fullTableName)) {
						loadBean.roleType = 2;
					} else if (config.sourceTableContractBene.equals(fullTableName)) {
						loadBean.roleType = 3;
					}
					loadBean.startSeq = beginSeq;
					loadBean.endSeq = beginSeq + SEQ_INTERVAL;
					loadBeanList.add(loadBean);

					beginSeq = beginSeq + SEQ_INTERVAL;
				}
				LoadBean loadBean = new LoadBean();
				loadBean.fullTableName = fullTableName;
				loadBean.roleType = 1;
				loadBean.startSeq = beginSeq;
				loadBean.endSeq = maxSeq;
				loadBeanList.add(loadBean);

				List<CompletableFuture<Map<String, String>>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> loadPartyContact(t.fullTableName, t.roleType, t.startSeq, t.endSeq), executor))
						.collect(Collectors.toList());			



				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

				//				for (Map<String, String> map : result) {
				//					String sourceTable = map.get("SOURCE_TABLE");
				//					String sinkTable = map.get("SINK_TABLE");
				//					String returnCode = map.get("RETURN_CODE");
				//					String recordCount = "";
				//					String errormsg = "";
				//					String stackTrace = "";
				//					
				//					if ("0".equals(returnCode)) {
				//						recordCount = map.get("RECORD_COUNT");
				//					} else {
				//						errormsg = map.get("ERROR_MSG");
				//						stackTrace = map.get("STACE_TRACE");
				//					}

				//					logger.info("sourceTable={}, sinkTable={}, returnCode={}, recordCount={}, errormsg={},stackTrace={}", 
				//							sourceTable, sinkTable, returnCode, recordCount, errormsg, stackTrace);
				//				}
			}
			 */

		} finally {
			if (executor != null) executor.shutdown();

		}
	}
	private void dropTable(String tableName) throws SQLException {
		Connection conn = sinkConnectionPool.getConnection();

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE " + tableName);
			stmt.close();
		} catch (java.sql.SQLException e) {
			logger.info(">>>  table:" + tableName + " does not exists!!!");
		} finally {
			if (conn != null) { 
				conn.close();
			}
		}
	}
	private void createTable(String tableName, String createTableFile) throws Exception {
		Connection conn = sinkConnectionPool.getConnection();

		Statement stmt = null;
		ClassLoader loader = Thread.currentThread().getContextClassLoader();	
		try (InputStream inputStream = loader.getResourceAsStream(createTableFile)) {
			String createTableScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			stmt = conn.createStatement();
			stmt.executeUpdate(createTableScript);
		} catch (SQLException | IOException e) {
			if (stmt != null) stmt.close();
			throw e;
		}

		conn.close();

	}

	private Integer createIndex(String sql) {

		Connection sinkConn = null;
		Statement stmt = null;
		int ret = 0;
		try {
			sinkConn = this.sinkConnectionPool.getConnection();
			stmt = sinkConn.createStatement();
			ret = stmt.executeUpdate(sql);

		} catch(Exception e) {
			logger.error(">>>error={}", ExceptionUtils.getStackTrace(e));
		} finally {
			try {
				if (stmt != null) stmt.close();
				if (sinkConn != null) sinkConn.close();
			}	catch(Exception e) {
				logger.error(">>>error={}", ExceptionUtils.getStackTrace(e));
			} 
		}
		return ret;
	}

	private void runCreateIndexes() {

		long t0 = System.currentTimeMillis();
		createIndex("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + config.sinkTablePartyContact + " (MOBILE_TEL) INLINE_SIZE 10 PARALLEL 8");
		logger.info(">>>>> create index mobile span={}", (System.currentTimeMillis() - t0));

		t0 = System.currentTimeMillis();
		createIndex("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + config.sinkTablePartyContact + " (EMAIL)  INLINE_SIZE 20 PARALLEL 8");
		logger.info(">>>>> create index email span={}", (System.currentTimeMillis() - t0));

		t0 = System.currentTimeMillis();
		createIndex("CREATE INDEX IDX_PARTY_CONTACT_TEMP_1 ON " + config.sinkTablePartyContactTemp + " (ADDRESS_ID) PARALLEL 8");
		logger.info(">>>>> create index temp address id span={}", (System.currentTimeMillis() - t0));


		/*
		List<String> indexSqlList = new ArrayList<>();
		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + config.sinkTablePartyContact + " (MOBILE_TEL)");
		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + config.sinkTablePartyContact + " (EMAIL)");
		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_3 ON " + config.sinkTablePartyContact + " (ADDRESS_ID)");
		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_TEMP_1 ON " + config.sinkTablePartyContactTemp + " (ADDRESS_ID)");
		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		List<CompletableFuture<Integer>> futures = 
				indexSqlList.stream().map(t -> CompletableFuture.supplyAsync(
						() -> createIndex(t), executor))
				.collect(Collectors.toList());			


		List<Integer> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
		 */
	}
}
