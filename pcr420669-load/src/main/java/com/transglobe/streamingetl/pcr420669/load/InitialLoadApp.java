package com.transglobe.streamingetl.pcr420669.load;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.pcr420669.load.model.InterestedPartyContact;

public class InitialLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	private static final String TABLE_POLICY_HOLDER = "PMUSER.T_POLICY_HOLDER";
	private static final String TABLE_INSURED_LIST = "PMUSER.T_INSURED_LIST";
	private static final String TABLE_CONTRACT_BENE = "PMUSER.T_CONTRACT_BENE";
	private static final String TABLE_ADDRESS = "PMUSER.T_ADDRESS";
	private static final String SINK_TABLE_INTERESTED_PARTY_CONTACT = "PMUSER.T_INTERESTED_PARTY_CONTACT";


	public static void main(String[] args) {
		InitialLoadApp app = new InitialLoadApp();
		try {
			DbConfig config = app.getDbConnConfig(CONFIG_FILE_NAME);

			test1(config);

			//ExecutorService executor = Executors.newFixedThreadPool(10);



		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	public DbConfig getDbConnConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);

			DbConfig dbConfig = new DbConfig();
			dbConfig.sourceDbDriver = prop.getProperty("source.db.driver");
			dbConfig.sourceDbUrl = prop.getProperty("source.db.url");
			dbConfig.sourceDbUsername = prop.getProperty("source.db.username");
			dbConfig.sourceDbPassword = prop.getProperty("source.db.password");

			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			dbConfig.sinkDbUsername = prop.getProperty("sink.db.username");
			dbConfig.sinkDbPassword = prop.getProperty("sink.db.password");

			return dbConfig;
		} catch (Exception e) {
			throw e;
		} 
	}
	private static void test2(DbConfig dbConfig) throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(3);
		try {

		List<String> tables = new ArrayList<String>();
		tables.add(TABLE_POLICY_HOLDER);
		tables.add(TABLE_INSURED_LIST);
		tables.add(TABLE_CONTRACT_BENE);

		
		List<CompletableFuture<List<InterestedPartyContact>>> futures = 
				tables.stream().map(tableName -> CompletableFuture.supplyAsync(
						() -> getInterestedPartyContact(dbConfig, tableName), executor))
				.collect(Collectors.toList());

		List<InterestedPartyContact> result = futures.stream().map(CompletableFuture::join)
				 .flatMap(List::stream).collect(Collectors.toList());
		
		// save result to sink db
		
		
		} finally {
			if (executor != null) executor.shutdown();
		}

	}
	private static void saveToSink(DbConfig dbConfig, List<InterestedPartyContact> contactList) {
		Connection sinkConn = null;
		
		try {
			Class.forName(dbConfig.sinkDbDriver);
			sinkConn = DriverManager.getConnection(dbConfig.sinkDbUrl, dbConfig.sinkDbUsername, dbConfig.sinkDbPassword);

			String sql = "INSERT INTO " + SINK_TABLE_INTERESTED_PARTY_CONTACT + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) "
					+ " values (?,?,?,?,?,?,?,?,?)";
			PreparedStatement pstmt = sinkConn.prepareStatement(sql);	
			
			int count = 0;
			for (InterestedPartyContact contact : contactList) {
				count++;
				
				pstmt.setInt(1, contact.getRoleType());
				pstmt.setLong(2, contact.getListId());
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
				
				if (count % 100 == 0 || count == contactList.size()) {
					pstmt.executeBatch();//executing the batch  
					
					logger.info("   >>>count={}, execute batch", count);
				}
			}
			if (pstmt != null) pstmt.close();
			
			sinkConn.commit();  
		
		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		} finally {
			if (sinkConn != null) {
				try {
					sinkConn.close();
				} catch (SQLException e) {
					logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
		}
	}
	private static List<InterestedPartyContact> getInterestedPartyContact(DbConfig dbConfig, String tablename){
		Connection sourceConn = null;
		List<InterestedPartyContact> contactList = new ArrayList<>();
		try {
			Class.forName(dbConfig.sourceDbDriver);
			sourceConn = DriverManager.getConnection(dbConfig.sourceDbUrl, dbConfig.sourceDbUsername, dbConfig.sourceDbPassword);

			String sql = "";
			if (TABLE_POLICY_HOLDER.equals(tablename)) {
				sql = "select 1 as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + TABLE_POLICY_HOLDER + " a inner join t_address b on a.address_id = b.address_id";
			} else if (TABLE_INSURED_LIST.equals(tablename)) {
				sql = "select 2 as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + TABLE_INSURED_LIST + " a inner join t_address b on a.address_id = b.address_id";
			} else if (TABLE_CONTRACT_BENE.equals(tablename)) {
				sql = "select 3 as ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from " + TABLE_CONTRACT_BENE + " a inner join t_address b on a.address_id = b.address_id";
			} 
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

		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
		}
		return contactList;
	}
	private static void test1(DbConfig dbConfig) throws Exception {
		Connection sourceConn = null;
		try {
			Class.forName(dbConfig.sourceDbDriver);
			sourceConn = DriverManager.getConnection(dbConfig.sourceDbUrl, dbConfig.sourceDbUsername, dbConfig.sourceDbPassword);

			// truncate table
			Statement stmt = sourceConn.createStatement();
			ResultSet resultSet = stmt.executeQuery(
					"select a.ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from t_policy_holder a inner join t_address b on a.address_id = b.address_id \n" + 
							" union all\n" + 
							" select a.ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from t_insured_list a inner join t_address b on a.address_id = b.address_id \n" + 
							" union all\n" + 
					"  select a.ROLE_TYPE,a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1 from t_contract_bene a inner join t_address b on a.address_id = b.address_id \n" );


			resultSet.close();
			stmt.close();
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
}
