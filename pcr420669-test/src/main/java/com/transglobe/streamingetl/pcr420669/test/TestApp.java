package com.transglobe.streamingetl.pcr420669.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.test.model.PartyContact;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;


public class TestApp {
	private static final Logger logger = LoggerFactory.getLogger(TestApp.class);

	private static String BASE_URL = "http://localhost:8080/partycontact/v1.0";

	private static int ADDRESS_ROLE_TYPE = 0;
	private static int POLICY_HOLDER_ROLE_TYPE = 1;
	private static int INSURED_LIST_ROLE_TYPE = 2;
	private static int CONTRACT_BENE_ROLE_TYPE = 3;

	private static String ADDRESS_SRC = "TEST_T_ADDRESS1";
	private static String POLICY_HOLDER_SRC = "TEST_T_POLICY_HOLDER1";
	private static String INSURED_LIST_SRC = "TEST_T_INSURED_LIST1";
	private static String CONTRACT_BENE_SRC = "TEST_T_CONTRACT_BENE1";


	private static final String CONFIG_FILE_NAME = "config.dev1.properties";

	Config config;

	public TestApp (String fileName) throws Exception {
		config = Config.getConfig(fileName);
	}

	public static void main(String[] args) {
		TestApp app;

		try {
			app = new TestApp(CONFIG_FILE_NAME);

			app.testInit();

			app.testInsert1Party(POLICY_HOLDER_ROLE_TYPE);

			app.testInsert1Party(INSURED_LIST_ROLE_TYPE);
			
			app.testInsert1Party(CONTRACT_BENE_ROLE_TYPE);


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.getMessage());
		}




	}
	private void testInit() throws Exception {
		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmt = null;
		String sql = "";
		try {
			Class.forName(config.sourceDbDriver);
			//	logger.info(">>driver={}, sourceDbUrl={},sourceDbUsername={},sourceDbPassword={}", config.sourceDbDriver, config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);

			Class.forName(config.sinkDbDriver);
			//	logger.info(">>driver={}, sinkDbUrl={},sinkDbUsername={},sinkDbPassword={}", config.sinkDbDriver, config.sinkDbUrl);
			sinkConn = DriverManager.getConnection(config.sinkDbUrl, null, null);

			sql = "delete " + config.sinkTablePartyContact;
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.executeUpdate();	
			pstmt.close();	

			List<String> sqlList = new ArrayList<>();
			sqlList.add("truncate table " + config.sourceTablePolicyHolder);
			sqlList.add("truncate table " + config.sourceTableInsuredList);
			sqlList.add("truncate table " + config.sourceTableContractBene);
			for (String sqlstr : sqlList) {
				pstmt = sourceConn.prepareStatement(sqlstr);
				pstmt.executeUpdate();	
				pstmt.close();	
			}
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
	private void testInsert1Party(int roleType) throws Exception {
		logger.info(">>>>> Start --> testInsert1Party roleType={}", roleType);

		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs;
		String sql = "";
		try {
			Class.forName(config.sourceDbDriver);
			//	logger.info(">>driver={}, sourceDbUrl={},sourceDbUsername={},sourceDbPassword={}", config.sourceDbDriver, config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);

			Class.forName(config.sinkDbDriver);
			//	logger.info(">>driver={}, sinkDbUrl={},sinkDbUsername={},sinkDbPassword={}", config.sinkDbDriver, config.sinkDbUrl);
			sinkConn = DriverManager.getConnection(config.sinkDbUrl, null, null);

			sourceConn.setAutoCommit(false);
			sinkConn.setAutoCommit(false);

			String initSrcTable = "";
			String srcTable = "";

			sql = "";
			if (roleType == 1) {
				initSrcTable = POLICY_HOLDER_SRC;
				srcTable = config.sourceTablePolicyHolder;
			} else if (roleType == 2) {
				initSrcTable = INSURED_LIST_SRC;
				srcTable = config.sourceTableInsuredList;
			} else if (roleType == 3) {
				initSrcTable = CONTRACT_BENE_SRC;
				srcTable = config.sourceTableContractBene;
			} else {
				throw new Exception(">>>>> error roleType=" + roleType);
			}


			sql = "select list_id from " + initSrcTable;
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery(sql);
			List<Long> listIdList = new ArrayList<>();
			while (rs.next()) {
				listIdList.add(rs.getLong("LIST_ID"));
			}

			Random random = new Random(System.currentTimeMillis());
			int offset  = random.nextInt(listIdList.size());

			Long selectedListId = listIdList.get(offset);

			logger.info(">>count={}, offset={}, selectedListId={}", 
					listIdList.size(), offset, selectedListId);

			sql = "insert into " + srcTable 
					+ " (select * from " + initSrcTable 
					+ " where list_id = "
					+ selectedListId
					+ ")";

			pstmt = sourceConn.prepareStatement(sql);
			pstmt.executeUpdate();	
			pstmt.close();
			sourceConn.commit();

			logger.info(">>> insert sql={}", sql);

			// check data insert oracle
			sql = "select * from " + srcTable;
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery(sql);
			int i = 0;
			PartyContact partyContact = new PartyContact();
			while (rs.next()) {
				i++;
				partyContact.setRoleType(roleType);
				partyContact.setListId(rs.getLong("LIST_ID"));
				partyContact.setPolicyId(rs.getLong("POLICY_ID"));
				partyContact.setName(rs.getString("NAME"));
				partyContact.setCertiCode(rs.getString("CERTI_CODE"));
				partyContact.setMobileTel(rs.getString("MOBILE_TEL"));
				if (CONTRACT_BENE_ROLE_TYPE == roleType) {
					partyContact.setEmail(null); // 受益人無email欄位輸入，故不比對他件受益人的email
				} else {
					partyContact.setEmail(rs.getString("EMAIL"));
				}
				partyContact.setAddressId(rs.getLong("ADDRESS_ID"));
				partyContact.setAddress1(null);
			}
			rs.close();
			pstmt.close();

			if (i != 1) {
				throw new Exception(">>>>> testInsert1Party error, oracle wrong data row count:" + i);
			}

			// check data insert ignite
			sql = "select * from " + config.sinkTablePartyContact + " where list_id = " + partyContact.getListId();
			Statement stmt = sinkConn.createStatement();
			int j = 0;
			PartyContact partyContact2 = new PartyContact();
			while (true) {
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					j++;
					partyContact2.setRoleType(rs.getInt("ROLE_TYPE"));
					partyContact2.setListId(rs.getLong("LIST_ID"));
					partyContact2.setPolicyId(rs.getLong("POLICY_ID"));
					partyContact2.setName(rs.getString("NAME"));
					partyContact2.setCertiCode(rs.getString("CERTI_CODE"));
					partyContact2.setMobileTel(rs.getString("MOBILE_TEL"));
					partyContact2.setEmail(rs.getString("EMAIL"));
					partyContact2.setAddressId(rs.getLong("ADDRESS_ID"));
					partyContact2.setAddress1(rs.getString("ADDRESS_1"));
				}
				if (j > 0) {
					break;
				} else  {
					logger.info(">>> Wait for 2 seconds");
					Thread.sleep(2000);
				}

			}
			rs.close();
			stmt.close();

			if (j != 1) {
				throw new Exception(">>>>> testInsert1Party contact error, ignite wrong data row count:" + j);
			}
			if (!partyContact.equals(partyContact2)) {
				throw new Exception(">>>>> partyContact <> partyContact2");
			}
			logger.info(">>>>> testInsert1Party, Table insert");

			
			// check spring boot result for email
			List<PartyContact> contactLista = queryPartyContact("email", partyContact.getEmail());
			int retCounta = contactLista.size();
			if ( StringUtils.isBlank(partyContact.getEmail())) {
				if ( retCounta != 0) {
					throw new Exception(">>>>> testInsert1Party size for email check error, return from springboot wrong data row count:" + retCounta);
				}
			} else {
				boolean result = false;
				for (int k =0; k < retCounta; k++) {
					PartyContact partyContact3a = contactLista.get(k);
					if (partyContact.getListId().equals(partyContact3a.getListId())) {
						result = true;
						break;
					}
					
				}
				if (!result) {
					throw new Exception(">>>>> email found no match");
				}
			}


			// check spring boot result for mobileTel
			List<PartyContact> contactListb = queryPartyContact("mobileTel", partyContact.getMobileTel());
			int retCountb = contactListb.size();
			if ( StringUtils.isBlank(partyContact.getMobileTel())) {
				if ( retCountb != 0) {
					throw new Exception(">>>>> testInsert1Party size for mobileTel check error, return from springboot wrong data row count:" + retCountb);
				}
			} else {
				boolean result = false;
				for (int k =0; k < retCountb; k++) {
					PartyContact partyContact3b = contactListb.get(k);
					if (partyContact.getListId().equals(partyContact3b.getListId())) {
						result = true;
						break;
					}
				}
				if (!result) {
					throw new Exception(">>>>> email found no match");
				}
			}

			// check spring boot result for address
			List<PartyContact> contactListc = queryPartyContact("address", partyContact.getAddress1());
			int retCountc = contactListc.size();
			if ( StringUtils.isBlank(partyContact.getAddress1())) {
				if ( retCountc != 0) {
					throw new Exception(">>>>> testInsert1Party size for address check error, return from springboot wrong data row count:" + retCountc);
				}
			} else {
				boolean result = false;
				for (int k =0; k < retCountc; k++) {
					PartyContact partyContact3c = contactListc.get(k);
					if (partyContact.getListId().equals(partyContact3c.getListId())) {
						result = true;
						break;
					}
				}
				if (!result) {
					throw new Exception(">>>>> address found no match");
				}
			}

			logger.info(">>>>> End -> TEST testInsert1Party, Query     [ OK ]");

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

	private List<PartyContact> queryPartyContact(String searchBy, String searchContent) throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		OkHttpClient client = new OkHttpClient();

		HttpUrl.Builder urlBuilder = HttpUrl.parse(BASE_URL + "/search").newBuilder();
		urlBuilder.addQueryParameter(searchBy, searchContent);

		String url = urlBuilder.build().toString();

		Request request = new Request.Builder()
				.url(url)
				.build();

		Call call = client.newCall(request);
		Response response = call.execute();

		if (response.code() != 200) {
			throw new Exception(">>>>> spring boot respond errpr! code=" + response.code());
		}
		String jsonString = response.body().string();
		logger.info(">>>>> searchby={}, response:{}", searchBy, jsonString);

		List<PartyContact> contactList = new ArrayList<>();
		contactList = mapper.readValue(jsonString, new TypeReference<List<PartyContact>>(){});

		return contactList;
	}
}
