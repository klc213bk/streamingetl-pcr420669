package com.transglobe.streamingetl.pcr420669.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.test.model.PartyContact;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;


public class TestApp {
	private static final Logger logger = LoggerFactory.getLogger(TestApp.class);
	
	private static Integer ADDRESS_ROLE_TYPE = 0;
	private static Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static Integer INSURED_LIST_ROLE_TYPE = 2;
	private static Integer CONTRACT_BENE_ROLE_TYPE = 3;
	
	private static String ADDRESS_SRC = "TEST_T_ADDRESS1";
	private static String POLICY_HOLDER_SRC = "TEST_T_POLICY_HOLDER1";
	private static String INSURED_LIST_SRC = "TEST_T_INSURED_LIST1";
	private static String CONTRACT_BENE_SRC = "TEST_T_CONTRACT_BENE1";
	
	
	private static final String CONFIG_FILE_NAME = "config.dev2.properties";
	
	Config config;
	
	public TestApp (String fileName) throws Exception {
		config = Config.getConfig(fileName);
	}
	
	public static void main(String[] args) {
		TestApp app;
		
		try {
			app = new TestApp(CONFIG_FILE_NAME);
			
			app.testInsert1PolicyHolder();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
		
		
		
	}
	private void testInsert1PolicyHolder() throws Exception {
		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs;
		String sql = "";
		try {
			Class.forName(config.sourceDbDriver);
			logger.info(">>driver={}, sourceDbUrl={},sourceDbUsername={},sourceDbPassword={}", config.sourceDbDriver, config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);
			
			sql = "truncate table test_t_policy_holder";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.executeUpdate();
			
			pstmt.close();

			sql = "select count(*) as COUNT from "  + POLICY_HOLDER_SRC;
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery(sql);
			Integer count = 0;
			while (rs.next()) {
				count = rs.getInt("COUNT");
			}
			rs.close();
			pstmt.close();
			
			Random random = new Random(System.currentTimeMillis());
			int offset  = random.nextInt(count);
			
			logger.info(">>count={}, offset={}", count, offset);
			
			//for before 12c 
			sql = "insert into " + config.sourceTablePolicyHolder 	
				+ "( select * from " + POLICY_HOLDER_SRC + "\n"
				+ " where list_id in \n"
				+ " ("	
				+ " select list_id "
				+ "	  from (select rownum bRn "
				+ "	             , b.* "
				+ "	          from (select rownum aRn "
				+ "	                     , a.* "
				+ "	                  from " + POLICY_HOLDER_SRC + " a "
				+ "	                 order by a.name "
				+ "	               ) b "
				+ "	       ) "
				+ "	 where bRn between " + offset + " and "  + offset
				+ "))";
			
			//for after 12c 
			/*		
			sql = "insert into " + config.sourceTablePolicyHolder 
					+ " (select * from " + POLICY_HOLDER_SRC 
					+ " order by list_id "
					+ " offset " + offset  + " rows "
					+ " fetch next 1 row only "
					+ ")";
					*/
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.executeUpdate();
			
			pstmt.close();
			
			// check data insert 
			sql = "select * from test_t_policy_holder";
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery(sql);
			int i = 0;
			PartyContact partyContact = new PartyContact();
			while (rs.next()) {
				i++;
				partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
				partyContact.setListId(rs.getLong("LIST_ID"));
				partyContact.setPolicyId(rs.getLong("POLICY_ID"));
				partyContact.setName(rs.getString("NAME"));
				partyContact.setCertiCode(rs.getString("CERTI_CODE"));
				partyContact.setMobileTel(rs.getString("MOBILE_TEL"));
				partyContact.setEmail(rs.getString("EMAIL"));
				partyContact.setAddressId(rs.getLong("ADDRESS_ID"));
				partyContact.setAddress1(null);
			}
			rs.close();
			pstmt.close();
			
			if (i != 1) {
				throw new Exception(">>>>> testInsert1PolicyHolder error, wrong data row count");
			}
			logger.info(">>>>> TEST testInsert1PolicyHolder, insert 1 record:{}", ToStringBuilder.reflectionToString(partyContact));
			
			// check spring boot result
			ObjectMapper mapper = new ObjectMapper();

			OkHttpClient client = new OkHttpClient();

			Request request = new Request.Builder()
			   .url("http://localhost:8080/partycontact/v1.0/search?email=" + partyContact.getEmail())
			   .build(); // defaults to GET

			Response response = client.newCall(request).execute();
			logger.info(">>>>> response:{}", response.body().string());
			
			PartyContact contactRes = mapper.readValue(response.body().byteStream(), PartyContact.class);

			if (partyContact.equals(contactRes)) {
				logger.error(">>>>> testInsert1PolicyHolder error, partyContact not equal!!! partyContact={}, partyContactRes={}", partyContact, contactRes );
				throw new Exception("testInsert1PolicyHolder error, partyContact not equal!!!");
			}
			logger.info(">>>>> TEST testInsert1PolicyHolder     [ OK ]");
			
		} catch (Exception e) {
			throw e;
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					throw e;
				}
			}
			if (sinkConn != null) {
				try {
					sinkConn.close();
				} catch (SQLException e) {
					throw e;
				}
			}
		}
	}
}
