package com.transglobe.streamingetl.pcr420669.load;

import java.io.BufferedReader;
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

public class InitialLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	private static final String CREATE_TABLE_FILE_NAME = "createtable-T_PARTY_CONTACT.sql";
	private static final String CREATE_TEMP_TABLE_FILE_NAME = "createtable-T_PARTY_CONTACT_TEMP.sql";

	private static final int THREADS = 10;

	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;

	static class LoadBean {
		String fullTableName;
		Integer roleType;
		Long startSeq;
		Long endSeq;
	}
	private Config config;

	public InitialLoadApp(String fileName) throws Exception {
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
		
		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;
			String createTableFile = StringUtils.isBlank(profileActive)? CREATE_TABLE_FILE_NAME : profileActive + "/" + CREATE_TABLE_FILE_NAME;
			String createTempTableFile = StringUtils.isBlank(profileActive)? CREATE_TEMP_TABLE_FILE_NAME : profileActive + "/" + CREATE_TEMP_TABLE_FILE_NAME;

			InitialLoadApp app = new InitialLoadApp(configFile);

			// create sink table
			app.dropTable(app.config.sinkTablePartyContact);
			app.dropTable(app.config.sinkTablePartyContactTemp);

			app.createTable(app.config.sinkTablePartyContact, createTableFile);
			app.createTable(app.config.sinkTablePartyContactTemp, createTempTableFile);

			if (loaddata) {
				app.run();
			}

			app.close();

			System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}



	private Map<String, String> loadPartyContact(String sourceTableName, Integer roleType, Long startSeq, Long endSeq, Long t0){
		//		logger.info(">>> run loadInterestedPartyContact, table={}, roleType={}", sourceTableName, roleType);

		Map<String, String> map = new HashMap<>();
		try {

			Connection sourceConn = this.sourceConnectionPool.getConnection();
			Connection sinkConn = this.sinkConnectionPool.getConnection();

			String sql = "select " + roleType + " as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + sourceTableName + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id "
					+ " where " + startSeq + " <= a.address_id and a.address_id < " + endSeq ;
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
				String email = resultSet.getString("EMAIL");
				Long addressId = resultSet.getLong("ADDRESS_ID");
				String address1 = resultSet.getString("ADDRESS_1");

				pstmt.setInt(1, resultSet.getInt("ROLE_TYPE"));
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
			logger.info("   >>>roletype={}, startSeq={}, count={}, span={} ", roleType, startSeq, count, (System.currentTimeMillis() - t0));

			pstmt.executeBatch();
			if (pstmt != null) pstmt.close();
			if (count > 0) sinkConn.commit(); 

			resultSet.close();
			stmt.close();

			sourceConn.close();
			sinkConn.close();

			map.put("RETURN_CODE", "0");
			map.put("SOURCE_TABLE", sourceTableName);
			map.put("SINK_TABLE", config.sinkTablePartyContact);
			map.put("RECORD_COUNT", String.valueOf(count));

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

		Long t0 = System.currentTimeMillis();
		try {

			Connection sourceConn = this.sourceConnectionPool.getConnection();
			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet  = stmt.executeQuery("select max(address_id) as MAX_ADDRESS_ID from " + config.sourceTableAddress);
			Long maxSeq = 0L;
			while (resultSet.next()) {
				maxSeq = resultSet.getLong("MAX_ADDRESS_ID");
			}
			maxSeq = maxSeq++;
			resultSet.close();
			stmt.close();
			sourceConn.close();
			logger.info(">>> max address id={}", maxSeq);

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
								() -> loadPartyContact(t.fullTableName, t.roleType, t.startSeq, t.endSeq, t0), executor))
						.collect(Collectors.toList());			



				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

				logger.info("load span={}, ", (System.currentTimeMillis() - t0));

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

            // create indexes
			createIndexes();
			
			// check correction
			// sink
			Long t1 = System.currentTimeMillis();
			Connection sinkConn = this.sinkConnectionPool.getConnection();
			stmt = sinkConn.createStatement();
			resultSet = stmt.executeQuery("select count(*) as SINK_COUNT from " + this.config.sinkTablePartyContact );
			Long sinkCount = 0L;
			while (resultSet.next()) {
				sinkCount = resultSet.getLong("SINK_COUNT");
			}
			resultSet.close();
			stmt.close();
			sinkConn.close();

			// source 1
			sourceConn = this.sourceConnectionPool.getConnection();
			stmt = sourceConn.createStatement();
			resultSet = stmt.executeQuery("select count(*) as SOURCE_COUNT1 from " + this.config.sourceTablePolicyHolder + " a inner join " + this.config.sourceTableAddress + " b on a.address_id = b.address_id" );
			Long sourceCount1 = 0L;
			while (resultSet.next()) {
				sourceCount1 = resultSet.getLong("SOURCE_COUNT1");
			}
			resultSet.close();
			stmt.close();
			sourceConn.close();

			// source 2
			sourceConn = this.sourceConnectionPool.getConnection();
			stmt = sourceConn.createStatement();
			resultSet = stmt.executeQuery("select count(*) as SOURCE_COUNT2 from " + this.config.sourceTableInsuredList + " a inner join " + this.config.sourceTableAddress + " b on a.address_id = b.address_id" );
			Long sourceCount2 = 0L;
			while (resultSet.next()) {
				sourceCount2 = resultSet.getLong("SOURCE_COUNT2");
			}
			resultSet.close();
			stmt.close();
			sourceConn.close();

			// source 3
			sourceConn = this.sourceConnectionPool.getConnection();
			stmt = sourceConn.createStatement();
			resultSet = stmt.executeQuery("select count(*) as SOURCE_COUNT3 from " + this.config.sourceTableContractBene + " a inner join " + this.config.sourceTableAddress + " b on a.address_id = b.address_id" );
			Long sourceCount3 = 0L;
			while (resultSet.next()) {
				sourceCount3 = resultSet.getLong("SOURCE_COUNT3");
			}
			resultSet.close();
			stmt.close();
			sourceConn.close();

			Long sourceTotal = sourceCount1 + sourceCount2 + sourceCount3;
			logger.info("sink count={}, source total={}, count1={}, count2={}, count3={}, span={}", sinkCount, sourceTotal, sourceCount1, sourceCount2, sourceCount3, (System.currentTimeMillis() - t1)); 

		} finally {
			if (executor != null) executor.shutdown();

		}
	}
	private void dropTable(String tableName) throws SQLException {
		logger.info(">>>  dropTable, tableName={}", tableName);
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
		logger.info(">>>  createTable, tableName={}, createTableFile={}", tableName, createTableFile);
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

	private void createIndexes() throws SQLException {
		Connection sinkConn = null;
		Statement stmt = null;
		try {
			sinkConn = this.sinkConnectionPool.getConnection();
			stmt = sinkConn.createStatement();
			stmt.executeUpdate("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + this.config.sinkTablePartyContact + " (MOBILE_TEL)");
			stmt.executeUpdate("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + this.config.sinkTablePartyContact + " (EMAIL)");
			stmt.executeUpdate("CREATE INDEX IDX_PARTY_CONTACT_3 ON " + this.config.sinkTablePartyContact + " (ADDRESS_ID)");
			stmt.executeUpdate("CREATE INDEX IDX_PARTY_CONTACT_TEMP_1 ON " + this.config.sinkTablePartyContactTemp + " (ADDRESS_ID)");
		} finally {
			if (stmt != null) stmt.close();
			if (sinkConn != null) sinkConn.close();
		}
	}
}
