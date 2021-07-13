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
 *  
 *  select  a.LIST_ID,a.POLICY_ID,a.NAME, 
a.CERTI_CODE,a.MOBILE_TEL, a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1
  from %tlogtable% a 
  left join T_ADDRESS b on a.ADDRESS_ID = b.ADDRESS_ID 
  where LAST_CMT_FLG = 'Y'
union  
select a.LIST_ID,a.POLICY_ID,a.NAME, a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1
  from %ttable% A
 inner join T_CONTRACT_MASTER B ON A.POLICY_ID=B.POLICY_ID 
  left join T_ADDRESS c on A.ADDRESS_ID = C.ADDRESS_ID 
  where B.LIABILITY_STATE = 0;


where %ttable% = T_POLICY_HOLDER,T_INSURED_LIST,T_CONTRACT_BENE 
and %tlogtable% = T_POLICY_HOLDER_LOG,T_INSURED_LIST_LOG,T_CONTRACT_BENE_LOG

 * @author oracle
 *
 */
public class InitialLoadApp2 {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp2.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	private static final String CREATE_TABLE_FILE_NAME = "createtable-T_PARTY_CONTACT.sql";
	private static final String CREATE_STREAMING_ETL_HEALTH_TABLE_FILE_NAME = "createtable-T_STREAMING_ETL_HEALTH.sql";
	
	private static final int THREADS = 15;

	//	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;
	
	public String sourceTablePolicyHolder;
	public String sourceTableInsuredList;
	public String sourceTableContractBene;
	public String sourceTablePolicyHolderLog;
	public String sourceTableInsuredListLog;
	public String sourceTableContractBeneLog;

	public String sourceTableContractMaster;
	public String sourceTableAddress;

	public String sinkTablePartyContact;
	public String sinkTableStreamingEtlHealth;

	static class LoadBean {
		String tableName;
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

		sourceTablePolicyHolder = config.sourceTablePolicyHolder;
		sourceTableInsuredList = config.sourceTableInsuredList;
		sourceTableContractBene = config.sourceTableContractBene;
		sourceTablePolicyHolderLog = config.sourceTablePolicyHolderLog;
		sourceTableInsuredListLog = config.sourceTableInsuredListLog;
		sourceTableContractBeneLog = config.sourceTableContractBeneLog;

		sourceTableContractMaster = config.sourceTableContractMaster;
		sourceTableAddress = config.sourceTableAddress;

		sinkTablePartyContact = config.sinkTablePartyContact;
		sinkTableStreamingEtlHealth = config.sinkTableStreamingEtlHealth;
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

		boolean noload = false;
		if (args.length != 0 && StringUtils.equals("noload", args[0])) {
			noload = true;
		}


		Long t0 = System.currentTimeMillis();

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;
			String createTableFile = CREATE_TABLE_FILE_NAME;
			String createStreamingEtlHealthTableFile = CREATE_STREAMING_ETL_HEALTH_TABLE_FILE_NAME;

			InitialLoadApp2 app = new InitialLoadApp2(configFile);

			// create sink table
			logger.info(">>>  Start: dropTable");
			app.dropTable(app.sinkTablePartyContact);
			app.dropTable(app.sinkTableStreamingEtlHealth);
			logger.info(">>>  End: dropTable DONE!!!");

			logger.info(">>>  Start: createTable");			
			app.createTable(app.sinkTablePartyContact, createTableFile);
			app.createTable(app.sinkTableStreamingEtlHealth, createStreamingEtlHealthTableFile);
			
			logger.info(">>>  End: createTable DONE!!!");

			logger.info("init tables span={}, ", (System.currentTimeMillis() - t0));						

			if (!noload) {
				app.run();

				app.runTLog();
			}
			logger.info("run load data span={}, ", (System.currentTimeMillis() - t0));

			// create indexes
			app.runCreateIndexes();


			app.close();


			logger.info("total load span={}, ", (System.currentTimeMillis() - t0));


			System.exit(0);

		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			System.exit(1);
		}

	}



	private Map<String, String> loadPartyContact(String sql, LoadBean loadBean){
		//		logger.info(">>> run loadInterestedPartyContact, table={}, roleType={}", sourceTableName, roleType);

		Console cnsl = null;
		Map<String, String> map = new HashMap<>();
		try {

			Connection sourceConn = this.sourceConnectionPool.getConnection();
			Connection sinkConn = this.sinkConnectionPool.getConnection();

			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet = stmt.executeQuery(sql);

			sinkConn.setAutoCommit(false); 
			PreparedStatement pstmt = sinkConn.prepareStatement(
					"insert into " + this.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
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
				String email = (loadBean.roleType == 3)? null : resultSet.getString("EMAIL");
				Long addressId = resultSet.getLong("ADDRESS_ID");
				String address1 = resultSet.getString("ADDRESS_1");

				if (listId.longValue() == 2437872) {
					logger.error("listid = 2437872, sql={}", sql); 
				}
				
				
				pstmt.setInt(1, loadBean.roleType);
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
					pstmt.setString(6, StringUtils.trim(mobileTel));
				}
				if (email == null) {
					pstmt.setNull(7, Types.VARCHAR);
				} else {
					pstmt.setString(7, StringUtils.trim(email.toLowerCase()));
				}

				pstmt.setLong(8, addressId);

				if (address1 == null) {
					pstmt.setNull(9, Types.VARCHAR);
				} else {
					pstmt.setString(9, StringUtils.trim(address1));
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
			cnsl.printf("   >>>roletype=%d, startSeq=%d, endSeq=%d, count=%d \n", loadBean.roleType, loadBean.startSeq, loadBean.endSeq, count);

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
			//			map.put("SQL", sourceTableName);
			//			map.put("SINK_TABLE", config.sinkTablePartyContact);
			//			map.put("RECORD_COUNT", String.valueOf(count));

		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			map.put("RETURN_CODE", "-999");
			map.put("SQL", sql);
			map.put("SINK_TABLE", this.sinkTablePartyContact);
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
					table = this.sourceTablePolicyHolder;
					roleType = 1;
				} else if ( i == 1) {
					table = this.sourceTableInsuredList;
					roleType = 2;
				} else if ( i == 2) {
					table = this.sourceTableContractBene;
					roleType = 3;
				}
				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " + table;
				
//				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " 
//				+ table + " where list_id >= 31000000";
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				long maxListId = 0;
				long minListId = 0;
				while (rs.next()) {
					minListId = rs.getLong("MIN_LIST_ID");
					maxListId = rs.getLong("MAX_LIST_ID");
				}
				rs.close();
				pstmt.close();

				long stepSize = 10000;
				long startIndex = minListId;

				int totalPartyCount = 0;
				List<LoadBean> loadBeanList = new ArrayList<>();
				while (startIndex <= maxListId) {
					long endIndex = startIndex + stepSize;


					int j = 0;
					long  subStepSize = stepSize;
					LoadBean loadBean = new LoadBean();
					loadBean.tableName = table;
					loadBean.roleType = roleType;
					loadBean.startSeq = startIndex + j * subStepSize;
					loadBean.endSeq = startIndex + (j + 1) * subStepSize;
					loadBeanList.add(loadBean);

					startIndex = endIndex;
				}

				logger.info("table={}, maxlistid={}, minListId={}, size={}, total partyCount={}", table, maxListId, minListId, loadBeanList.size(), totalPartyCount);

				List<CompletableFuture<Map<String, String>>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> {
										String sqlStr = "select a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1 from " 
											+ t.tableName 
											+ " a inner join " + this.sourceTableContractMaster + " b ON a.POLICY_ID=b.POLICY_ID "
											+ " left join " + this.sourceTableAddress + " c on a.address_id = c.address_id "
											+ " where b.LIABILITY_STATE = 0 and " + t.startSeq + " <= a.list_id and a.list_id < " + t.endSeq ;
										return loadPartyContact(sqlStr, t);
									}
								, executor)
								)
						.collect(Collectors.toList());			

				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			}

		} finally {
			if (executor != null) executor.shutdown();

		}
	}
	private void runTLog() throws Exception {

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
					table = this.sourceTablePolicyHolderLog;
					roleType = 1;
				} else if ( i == 1) {
					table = this.sourceTableInsuredListLog;
					roleType = 2;
				} else if ( i == 2) {
					table = this.sourceTableContractBeneLog;
					roleType = 3;
				}
				sql = "select min(log_id) as MIN_LOG_ID, max(log_id) as MAX_LOG_ID from " + table;
				
//				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " 
//				+ table + " where list_id >= 31000000";
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				long maxLogId = 0;
				long minLogId = 0;
				while (rs.next()) {
					minLogId = rs.getLong("MIN_LOG_ID");
					maxLogId = rs.getLong("MAX_LOG_ID");
				}
				rs.close();
				pstmt.close();

				long stepSize = 10000;
				long startIndex = minLogId;

				int totalPartyCount = 0;
				List<LoadBean> loadBeanList = new ArrayList<>();
				while (startIndex <= maxLogId) {
					long endIndex = startIndex + stepSize;


					int j = 0;
					long  subStepSize = stepSize;
					LoadBean loadBean = new LoadBean();
					loadBean.tableName = table;
					loadBean.roleType = roleType;
					loadBean.startSeq = startIndex + j * subStepSize;
					loadBean.endSeq = startIndex + (j + 1) * subStepSize;
					loadBeanList.add(loadBean);

					startIndex = endIndex;
				}

				logger.info("table={}, maxlistid={}, minListId={}, size={}, total partyCount={}", table, maxLogId, minLogId, loadBeanList.size(), totalPartyCount);

				List<CompletableFuture<Map<String, String>>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> {
										String sqlStr = "select a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1 from " 
											+ t.tableName 
											+ " a left join " + this.sourceTableAddress + " c on a.address_id = c.address_id "
											+ " where a.LAST_CMT_FLG = 'Y' and " + t.startSeq + " <= a.log_id and a.log_id < " + t.endSeq ;
										return loadPartyContact(sqlStr, t);
									}
								, executor)
								)
						.collect(Collectors.toList());			

				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			}

		} finally {
			if (executor != null) executor.shutdown();

		}
	}
	private void dropTable(String tableName) throws SQLException {
		Connection conn = sinkConnectionPool.getConnection();

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
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
		createIndex("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + this.sinkTablePartyContact + " (MOBILE_TEL) INLINE_SIZE 10 PARALLEL 8");
		logger.info(">>>>> create index mobile span={}", (System.currentTimeMillis() - t0));

		t0 = System.currentTimeMillis();
		createIndex("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + this.sinkTablePartyContact + " (EMAIL)  INLINE_SIZE 20 PARALLEL 8");
		logger.info(">>>>> create index email span={}", (System.currentTimeMillis() - t0));

		t0 = System.currentTimeMillis();
		createIndex("CREATE INDEX IDX_PARTY_CONTACT_3 ON " + this.sinkTablePartyContact + " (ADDRESS_1)  INLINE_SIZE 60 PARALLEL 8");
		logger.info(">>>>> create index address span={}", (System.currentTimeMillis() - t0));

		t0 = System.currentTimeMillis();
		createIndex("CREATE INDEX IDX_STREAMING_ETL_HEALTH_1 ON " + this.sinkTableStreamingEtlHealth + " (CDC_TIME) PARALLEL 8");
		logger.info(">>>>> create index streamingetlhealth cdc_time span={}", (System.currentTimeMillis() - t0));

		
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
