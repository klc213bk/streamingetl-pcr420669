package com.transglobe.streamingetl.pcr420669.test;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.test.model.TAddress;
import com.transglobe.streamingetl.pcr420669.test.model.PartyContact;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;


public class TestApp {
	private static final Logger logger = LoggerFactory.getLogger(TestApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	
	private static int POLICY_HOLDER_ROLE_TYPE = 1;
	private static int INSURED_LIST_ROLE_TYPE = 2;
	private static int CONTRACT_BENE_ROLE_TYPE = 3;

	private static String ADDRESS_SRC = "TEST_T_ADDRESS1";
	private static String POLICY_HOLDER_SRC = "TEST_T_POLICY_HOLDER1";
	private static String INSURED_LIST_SRC = "TEST_T_INSURED_LIST1";
	private static String CONTRACT_BENE_SRC = "TEST_T_CONTRACT_BENE1";




	Config config;

	public TestApp (String fileName) throws Exception {
		config = Config.getConfig(fileName);
		
	}

	public static void main(String[] args) {

		String profileActive = System.getProperty("profile.active", "");

		TestApp app;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new TestApp(configFile);

			app.testSamples();

			app.testRandomInsertSamples(100);
			
			app.testRandomUpdateSamples(50);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	public void testRandomInsertSamples(Integer inputSampleSize) throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>>> START -> testRandomInsertSamples");

		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs;
		String sql = "";

		try {
	
			testInit();

			Class.forName(config.sourceDbDriver);
			//	logger.info(">>driver={}, sourceDbUrl={},sourceDbUsername={},sourceDbPassword={}", config.sourceDbDriver, config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);
			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);

			Class.forName(config.sinkDbDriver);
			//	logger.info(">>driver={}, sinkDbUrl={},sinkDbUsername={},sinkDbPassword={}", config.sinkDbDriver, config.sinkDbUrl);
			sinkConn = DriverManager.getConnection(config.sinkDbUrl, null, null);

			sourceConn.setAutoCommit(false);
			sinkConn.setAutoCommit(false);

			int total = 0;
			for (int i = 0; i < 3; i++) {
				if (i == 0) {
					sql = "select count(*) as COUNT from " + POLICY_HOLDER_SRC;
				} else if (i  == 1) {
					sql = "select count(*) as COUNT from " + INSURED_LIST_SRC;
				} else {
					sql = "select count(*) as COUNT from " + CONTRACT_BENE_SRC;
				}
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					total = total + rs.getInt("COUNT");
				}
				rs.close();
				pstmt.close();
			}
			logger.info(">>> total={}", total);

			int sampleSize = (inputSampleSize == null)? total : inputSampleSize;
			Random random = new Random(System.currentTimeMillis());
			Set<Integer> offsetSet = new HashSet<>();

			int i = 0;
			while (i < sampleSize) {
				Integer offset = random.nextInt(total) + 1;
				if (!offsetSet.contains(offset)) {
					offsetSet.add(offset);
					i++;
					logger.info(">>> i={}, offset={}", i, offset);
				}
			}

			// select list_ids
			sql = "select 1 AS ROLE_TYPE, list_id, a.address_id, b.address_1 from " + POLICY_HOLDER_SRC + " a inner join " + ADDRESS_SRC + " b on a.address_id = b.address_id \n" + 
					"union\n" + 
					"select 2 AS ROLE_TYPE, list_id, a.address_id, b.address_1 from " + INSURED_LIST_SRC + " a inner join " + ADDRESS_SRC + " b on a.address_id = b.address_id \n" + 
					"union\n" + 
					"select 3 AS ROLE_TYPE, list_id, a.address_id, b.address_1 from " + CONTRACT_BENE_SRC + " a inner join " + ADDRESS_SRC + " b on a.address_id = b.address_id" ; 
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			List<Map<String, Object>> partymapList = new ArrayList<>();
			List<Long> addressIdList = new ArrayList<>();
			int count = 0;
			while (rs.next()) {
				count++;
				if (offsetSet.contains(count)) {
					Map<String, Object> partymap = new HashMap<>();
					partymap.put("ROLE_TYPE", rs.getInt("ROLE_TYPE"));
					partymap.put("LIST_ID", rs.getLong("LIST_ID"));
					partymap.put("ADDRESS_ID", rs.getLong("ADDRESS_ID"));
					partymap.put("ADDRESS_1", rs.getString("ADDRESS_1"));
					partymapList.add(partymap);

					addressIdList.add(rs.getLong("ADDRESS_ID"));
				} else {
					continue;
				}
			}
			rs.close();
			pstmt.close();

			Collections.shuffle(partymapList);
			Collections.shuffle(addressIdList);

			Set<Long> insertAddressSet = new HashSet<>();
			Long lastInsertAddressid = null;
			for (int k = 0; k < partymapList.size(); k++) {
				Integer roleType = (Integer)partymapList.get(k).get("ROLE_TYPE");
				Long listId = (Long)partymapList.get(k).get("LIST_ID");

				logger.info(">>> insert k={}, roletype={}, listid={}, address_id={} with address1={}, addressid2={} ", 
						k, 
						roleType, 
						listId, 
						partymapList.get(k).get("ADDRESS_ID"), 
						partymapList.get(k).get("ADDRESS_1"), 
						addressIdList.get(k));

				insertParty(sourceConn, roleType, listId);

				Long addressid = addressIdList.get(k);
				if (!insertAddressSet.contains(addressid)) {
					insertAddress(sourceConn, addressid);
					insertAddressSet.add(addressid);
					lastInsertAddressid = addressid;
				}

			}

			// check insert complete
			Integer countPartyContact = null;
			Integer countPartyContactTemp = null;
			Console cnsl = null;
			while (true) {
				sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact;
				pstmt = sinkConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				
				while (rs.next()) {
					countPartyContact = rs.getInt("COUNT");
				}
				rs.close();
				pstmt.close();

				if (countPartyContact == null) {
					Thread.sleep(20000);
					continue;
				} else if (countPartyContact.intValue() < sampleSize) {

				//	cnsl = System.console();
			//		logger.info(">>>> cnsl:={}", cnsl);
				//	cnsl.printf("   >>>should be equal, countPartyContact=%d, sampleSize=%d \n", countPartyContact, sampleSize);

				//	cnsl.flush();
//					logger.info(">>>>> should be equal, countPartyContact={}, sampleSize={}", countPartyContact, sampleSize);
					Thread.sleep(2000);
					continue;
				}
				sql = "select count(*) AS COUNT from " + config.sinkTablePartyContactTemp;
				pstmt = sinkConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				
				while (rs.next()) {
					countPartyContactTemp = rs.getInt("COUNT");
				}
				rs.close();
				pstmt.close();
				
				if (countPartyContactTemp == null || countPartyContact.intValue() == 0) {
					logger.info(">>>>> countPartyContactTemp > 0 , countPartyContactTemp={}", countPartyContactTemp);
					Thread.sleep(2000);
					continue;
				}
				break;
			}
			logger.info(">>> countPartyContact={}, countPartyContactTemp={}", countPartyContact, countPartyContactTemp);
			logger.info(">>>>> insert compplete!!!!!");

			Thread.sleep(30000);
			
			// verify
			for (int k = 0; k < partymapList.size(); k++) {
				Integer roleType = (Integer)partymapList.get(k).get("ROLE_TYPE");
				Long listId = (Long)partymapList.get(k).get("LIST_ID");
				String table = null;
				if (roleType.intValue() == 1) {
					table = config.sourceTablePolicyHolder;
				} else if (roleType.intValue() == 2) {
					table = config.sourceTableInsuredList;
				} else if (roleType.intValue() == 3) {
					table = config.sourceTableContractBene;
				}

				sql = "select a.list_id, a.policy_id, a.name, a.certi_code, a.mobile_tel, a.email, a.address_id, b.address_1 from " 
						+ table + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id "
						+ " where list_id = ?";
				pstmt = sourceConn.prepareStatement(sql);
				pstmt.setLong(1, listId);
				rs = pstmt.executeQuery();

				PartyContact partyContact = null;
				while (rs.next()) {
					partyContact = new PartyContact();
					partyContact.setAddress1(rs.getString("ADDRESS_1"));
					partyContact.setAddressId(rs.getLong("ADDRESS_ID"));
					partyContact.setCertiCode(rs.getString("CERTI_CODE"));
					partyContact.setEmail(  (roleType.intValue() == 3)? null : rs.getString("EMAIL"));
					partyContact.setListId(rs.getLong("LIST_ID"));
					partyContact.setMobileTel(rs.getString("MOBILE_TEL"));
					partyContact.setName(rs.getString("NAME"));
					partyContact.setPolicyId(rs.getLong("POLICY_ID"));
					partyContact.setRoleType(roleType);
				}
				rs.close();
				pstmt.close();

				if (partyContact == null) {
					throw new Exception("Found no record in source db table=" + table + ", list_id=" + listId);
				}

				// select from party contact
				sql = "select role_type, list_id, policy_id, name, certi_code, mobile_tel, email, address_id, address_1 from " + config.sinkTablePartyContact 
						+ " where role_type = ? and list_id = ?";
				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setLong(1, roleType);
				pstmt.setLong(2, listId);


				PartyContact partyContactIgnite = null;


				rs = pstmt.executeQuery();
				while (rs.next()) {
					//						logger.info(">>>  Hi Hi Hi v v v Hi v v address1={}", rs.getString("ADDRESS_1"));
					partyContactIgnite = new PartyContact();
					partyContactIgnite.setAddress1(rs.getString("ADDRESS_1"));
					partyContactIgnite.setAddressId(rs.getLong("ADDRESS_ID"));
					partyContactIgnite.setCertiCode(rs.getString("CERTI_CODE"));
					partyContactIgnite.setEmail(rs.getString("EMAIL"));
					partyContactIgnite.setListId(rs.getLong("LIST_ID"));
					partyContactIgnite.setMobileTel(rs.getString("MOBILE_TEL"));
					partyContactIgnite.setName(rs.getString("NAME"));
					partyContactIgnite.setPolicyId(rs.getLong("POLICY_ID"));
					partyContactIgnite.setRoleType(rs.getInt("ROLE_TYPE"));
				}
				rs.close();
				pstmt.close();

				if (partyContactIgnite == null) {
					logger.info(">>> sql={}, role_type={}, list_id={}", sql, roleType, listId);
					throw new Exception("Found no record in partyContact, role type=" + roleType + ", list_id=" + listId);
				}

				if (!partyContact.equals(partyContactIgnite)) {
					logger.info(">>> partycontact={}", ToStringBuilder.reflectionToString(partyContact));
					logger.info(">>> partycontactIgnite={}", ToStringBuilder.reflectionToString(partyContactIgnite));

					throw new Exception(" No match for source partyContact and ignite partycontact, lastInsertAddressid:" + lastInsertAddressid);
				}
				logger.info(">>>>> test ignite object equals for k={} DONE!! roleType={}, listId={}", k, roleType, listId);    
				
				// test spring boot
				List<PartyContact> contactList = this.queryPartyContact("listId", String.valueOf(listId));
				if (contactList.size() != 1) {
					throw new Exception("spring boot query result error, size=" + contactList.size() + ", listId=" + listId);
				}
				PartyContact iPartyContact = contactList.get(0);
				if (!iPartyContact.equals(partyContactIgnite)) {
					logger.info(">>> iPartyContact={}", ToStringBuilder.reflectionToString(iPartyContact));
					logger.info(">>> partycontactIgnite={}", ToStringBuilder.reflectionToString(partyContactIgnite));

					throw new Exception(" No match for spring boot partyContact and ignite partycontac, listid=" + listId);
				}
				
				logger.info(">>>>> test spring boot object equals for k={} DONE!! roleType={}, listId={}", k, roleType, listId);    
				

			}

			logger.info(">>>>>>>>>>> End -> testRandomInsertSamples size={}    [  OK  ]", partymapList.size());

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
	public void testRandomUpdateSamples(Integer inputSampleSize) throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>>> START -> testRandomUpdateSamples");

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

			int total = 0;
			for (int i = 0; i < 3; i++) {
				if (i == 0) {
					sql = "select count(*) as COUNT from " + config.sourceTablePolicyHolder;
				} else if (i  == 1) {
					sql = "select count(*) as COUNT from " + config.sourceTableInsuredList;
				} else {
					sql = "select count(*) as COUNT from " + config.sourceTableContractBene;
				}
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					total = total + rs.getInt("COUNT");
				}
				rs.close();
				pstmt.close();
			}
			logger.info(">>> total={}", total);

			int sampleSize = (inputSampleSize == null)? total : inputSampleSize;
			Random random = new Random(System.currentTimeMillis());
			Set<Integer> offsetSet = new HashSet<>();

			int i = 0;
			while (i < sampleSize) {
				Integer offset = random.nextInt(total) + 1;
				if (!offsetSet.contains(offset)) {
					offsetSet.add(offset);
					i++;
					logger.info(">>> i={}, offset={}", i, offset);
				}
			}

			// select list_ids
			sql = "select 1 AS ROLE_TYPE, list_id, a.policy_id, a.certi_code, a.email, a.mobile_tel, a.name, a.address_id, b.address_1 from " + config.sourceTablePolicyHolder + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id \n" + 
					"union\n" + 
					"select 2 AS ROLE_TYPE, list_id, a.policy_id, a.certi_code, a.email, a.mobile_tel, a.name, a.address_id, b.address_1 from " + config.sourceTableInsuredList + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id \n" + 
					"union\n" + 
					"select 3 AS ROLE_TYPE, list_id, a.policy_id, a.certi_code, a.email, a.mobile_tel, a.name, a.address_id, b.address_1 from " + config.sourceTableContractBene + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id" ; 
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			List<Map<String, Object>> partymapList = new ArrayList<>();
			int count = 0;
			while (rs.next()) {
				count++;
				if (offsetSet.contains(count)) {
					Map<String, Object> partymap1 = new HashMap<>();
					partymap1.put("ROLE_TYPE", rs.getInt("ROLE_TYPE"));
					partymap1.put("LIST_ID", rs.getLong("LIST_ID"));
					partymap1.put("UPDATE_COLUMN", "EMAIL");
					partymap1.put("OLD_VALUE", rs.getString("EMAIL"));
					String randEmail = RandomStringUtils.random(10, true, false) + "@gmail.com";
					partymap1.put("NEW_VALUE", randEmail);
					partymapList.add(partymap1);
					
					Map<String, Object> partymap2 = new HashMap<>();
					partymap2.put("ROLE_TYPE", rs.getInt("ROLE_TYPE"));
					partymap2.put("LIST_ID", rs.getLong("LIST_ID"));
					partymap2.put("UPDATE_COLUMN", "MOBILE_TEL");
					partymap2.put("OLD_VALUE", rs.getString("MOBILE_TEL"));
					partymap2.put("NEW_VALUE", "09"+RandomStringUtils.randomNumeric(8));
					partymapList.add(partymap2);
					
					Map<String, Object> partymap3 = new HashMap<>();
					partymap3.put("ADDRESS_ID", rs.getLong("ADDRESS_ID"));
					partymap3.put("UPDATE_COLUMN", "ADDRESS_1");
					partymap3.put("OLD_VALUE", rs.getString("ADDRESS_1"));
					partymap3.put("NEW_VALUE",  "紐約" + RandomStringUtils.randomAlphanumeric(10));
					partymapList.add(partymap3);
	
				} else {
					continue;
				}
			}
			rs.close();
			pstmt.close();

			Collections.shuffle(partymapList);

			String table = null;
			for (int k = 0; k < partymapList.size(); k++) {
				if (partymapList.get(k).get("ROLE_TYPE") != null) {
					Integer roleType = (Integer)partymapList.get(k).get("ROLE_TYPE");
					Long listId = (Long)partymapList.get(k).get("LIST_ID");
					String updateColumn = (String)partymapList.get(k).get("UPDATE_COLUMN");
					String newValue = (String)partymapList.get(k).get("NEW_VALUE");
								
					if (roleType.intValue() == 1) {
						table = config.sourceTablePolicyHolder;
					} else if (roleType.intValue() == 1) {
						table = config.sourceTableInsuredList;
					} else {
						table = config.sourceTableContractBene;
					}
					sql = "update " + table
							+ " set " + updateColumn + " = ? " 
							+ " where list_id = ?";
					pstmt = sourceConn.prepareStatement(sql);
					pstmt.setString(1, newValue);
					pstmt.setLong(2, listId);
					pstmt.executeUpdate();
					sourceConn.commit();
					
				} else {
					Long addressId = (Long)partymapList.get(k).get("ADDRESS_ID");
					String updateColumn = (String)partymapList.get(k).get("UPDATE_COLUMN");		
					String newValue = (String)partymapList.get(k).get("NEW_VALUE");
					sql = "update " + config.sourceTableAddress
							+ " set " + updateColumn + " = ? " 
							+ " where address_id = ?";
					pstmt = sourceConn.prepareStatement(sql);
					pstmt.setString(1, newValue);
					pstmt.setLong(2, addressId);
					pstmt.executeUpdate();
					sourceConn.commit();
				}
				Thread.sleep(100);
			}

			Thread.sleep(10000);
			
			// verify
			for (int k = 0; k < partymapList.size(); k++) {
				if (partymapList.get(k).get("ROLE_TYPE") == null) {
					continue;
				}
				Integer roleType = (Integer)partymapList.get(k).get("ROLE_TYPE");
				Long listId = (Long)partymapList.get(k).get("LIST_ID");

				if (roleType.intValue() == 1) {
					table = config.sourceTablePolicyHolder;
				} else if (roleType.intValue() == 2) {
					table = config.sourceTableInsuredList;
				} else if (roleType.intValue() == 3) {
					table = config.sourceTableContractBene;
				}

				sql = "select a.list_id, a.policy_id, a.name, a.certi_code, a.mobile_tel, a.email, a.address_id, b.address_1 from " 
						+ table + " a inner join " + config.sourceTableAddress + " b on a.address_id = b.address_id "
						+ " where list_id = ?";
				pstmt = sourceConn.prepareStatement(sql);
				pstmt.setLong(1, listId);
				rs = pstmt.executeQuery();

				PartyContact partyContact = null;
				while (rs.next()) {
					partyContact = new PartyContact();
					partyContact.setAddress1(rs.getString("ADDRESS_1"));
					partyContact.setAddressId(rs.getLong("ADDRESS_ID"));
					partyContact.setCertiCode(rs.getString("CERTI_CODE"));
					partyContact.setEmail(  (roleType.intValue() == 3)? null : rs.getString("EMAIL"));
					partyContact.setListId(rs.getLong("LIST_ID"));
					partyContact.setMobileTel(rs.getString("MOBILE_TEL"));
					partyContact.setName(rs.getString("NAME"));
					partyContact.setPolicyId(rs.getLong("POLICY_ID"));
					partyContact.setRoleType(roleType);
				}
				rs.close();
				pstmt.close();

				if (partyContact == null) {
					throw new Exception("Found no record in source db table=" + table + ", list_id=" + listId);
				}

				// select from party contact
				sql = "select role_type, list_id, policy_id, name, certi_code, mobile_tel, email, address_id, address_1 from " + config.sinkTablePartyContact 
						+ " where role_type = ? and list_id = ?";
				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setLong(1, roleType);
				pstmt.setLong(2, listId);


				PartyContact partyContactIgnite = null;


				rs = pstmt.executeQuery();
				while (rs.next()) {
					//						logger.info(">>>  Hi Hi Hi v v v Hi v v address1={}", rs.getString("ADDRESS_1"));
					partyContactIgnite = new PartyContact();
					partyContactIgnite.setAddress1(rs.getString("ADDRESS_1"));
					partyContactIgnite.setAddressId(rs.getLong("ADDRESS_ID"));
					partyContactIgnite.setCertiCode(rs.getString("CERTI_CODE"));
					partyContactIgnite.setEmail(rs.getString("EMAIL"));
					partyContactIgnite.setListId(rs.getLong("LIST_ID"));
					partyContactIgnite.setMobileTel(rs.getString("MOBILE_TEL"));
					partyContactIgnite.setName(rs.getString("NAME"));
					partyContactIgnite.setPolicyId(rs.getLong("POLICY_ID"));
					partyContactIgnite.setRoleType(rs.getInt("ROLE_TYPE"));
				}
				rs.close();
				pstmt.close();

				if (partyContactIgnite == null) {
					logger.info(">>> sql={}, role_type={}, list_id={}", sql, roleType, listId);
					throw new Exception("Found no record in partyContact, role type=" + roleType + ", list_id=" + listId);
				}

				if (!partyContact.equals(partyContactIgnite)) {
					logger.info(">>> partycontact={}", ToStringBuilder.reflectionToString(partyContact));
					logger.info(">>> partycontactIgnite={}", ToStringBuilder.reflectionToString(partyContactIgnite));

					throw new Exception(" No match for source partyContact and ignite partycontact");
				}
				logger.info(">>>>> test ignite object equals for k={} DONE!! roleType={}, listId={}", k, roleType, listId);   
				
				// test spring boot
				List<PartyContact> contactList = this.queryPartyContact("listId", String.valueOf(listId));
				if (contactList.size() != 1) {
					throw new Exception("spring boot query result error, size=" + contactList.size() + ", listId=" + listId);
				}
				PartyContact iPartyContact = contactList.get(0);
				if (!iPartyContact.equals(partyContactIgnite)) {
					logger.info(">>> iPartyContact={}", ToStringBuilder.reflectionToString(iPartyContact));
					logger.info(">>> partycontactIgnite={}", ToStringBuilder.reflectionToString(partyContactIgnite));

					throw new Exception(" No match for spring boot partyContact and ignite partycontact, listid=" + listId);
				}
				
				logger.info(">>>>> test spring boot object equals for k={} DONE!! roleType={}, listId={}", k, roleType, listId);    
				
			}

			logger.info(">>>>>>>>>>> End -> testRandomUpdateSamples size={}    [  OK  ]", partymapList.size());

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

	public void testSamples() throws Exception {

		try {

			testInit();

			testInsert1NewParty(POLICY_HOLDER_ROLE_TYPE);

			testInsert1NewParty(INSURED_LIST_ROLE_TYPE);

			testInsert1NewParty(CONTRACT_BENE_ROLE_TYPE);

			testInsert1NewAddress();

			testInsert1AddressMatchPartyWithoutAddress();

			testInsert1PartyMatchAddressInTemp();

			testInsert1PartyMatchPartyWithAddress1();

			updateAddress1ForPartyContact();

			updateContactForPartyContact(POLICY_HOLDER_ROLE_TYPE, "EMAIL", "klc213@gmail.com");

			updateContactForPartyContact(INSURED_LIST_ROLE_TYPE, "MOBILE_TEL", "0909906222");

			updateContactForPartyContact(CONTRACT_BENE_ROLE_TYPE, "MOBILE_TEL", "0938383838");


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
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
			sinkConn.setAutoCommit(false);

			sql = "delete " + config.sinkTablePartyContact;
			logger.info(">>>>> sql1:{}", sql);
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.executeUpdate();	
			pstmt.close();

			sql = "delete " + config.sinkTablePartyContactTemp;
			logger.info(">>>>> sql2:{}", sql);
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.executeUpdate();	
			pstmt.close();
			sinkConn.commit();

			List<String> sqlList = new ArrayList<>();

			sqlList.add("truncate table " + config.sourceTablePolicyHolder);
			sqlList.add("truncate table " + config.sourceTableInsuredList);
			sqlList.add("truncate table " + config.sourceTableContractBene);
			sqlList.add("truncate table " + config.sourceTableAddress);
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
	private void testInsert1NewParty(int roleType) throws Exception {
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
			sql = "select * from " + srcTable + " where list_id = " + selectedListId;
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
			logger.info(">>> got party ={}", partyContact);

			// check data insert ignite
			sql = "select * from " + config.sinkTablePartyContact + " where list_id = " + partyContact.getListId();
			Statement stmt = sinkConn.createStatement();

			logger.info(">>> quert ignite, sql ={}", sql);

			List<PartyContact> partyContactList = new ArrayList<>();
			while (true) {
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					PartyContact partyContact2 = new PartyContact();
					partyContact2.setRoleType(rs.getInt("ROLE_TYPE"));
					partyContact2.setListId(rs.getLong("LIST_ID"));
					partyContact2.setPolicyId(rs.getLong("POLICY_ID"));
					partyContact2.setName(rs.getString("NAME"));
					partyContact2.setCertiCode(rs.getString("CERTI_CODE"));
					partyContact2.setMobileTel(rs.getString("MOBILE_TEL"));
					partyContact2.setEmail(rs.getString("EMAIL"));
					partyContact2.setAddressId(rs.getLong("ADDRESS_ID"));
					partyContact2.setAddress1(rs.getString("ADDRESS_1"));

					partyContactList.add(partyContact2);
				}
				if (partyContactList.size() > 0) {
					break;
				} else  {
					logger.info(">>> Wait for 2 seconds");
					Thread.sleep(2000);
				}

			}
			rs.close();
			stmt.close();

			if (partyContactList.size() != 1) {
				throw new Exception(">>>>> testInsert1Party contact error, ignite wrong data row count:" + partyContactList.size());
			}
			if (!partyContact.equals(partyContactList.get(0))) {
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

			logger.info(">>>>> End -> TEST testInsert1Party for {}, Query     [ OK ]", roleType);

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

		HttpUrl.Builder urlBuilder = HttpUrl.parse(config.webBaseurl + "/search").newBuilder();
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

	private void testInsert1NewAddress() throws Exception {
		logger.info(">>>>> Start --> testInsert1Address ");

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

			//			String initSrcTable = ADDRESS_SRC;
			//			String srcTable = config.sourceTableAddress;

			sql = "select address_id from " + ADDRESS_SRC;
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery(sql);
			List<Long> addressIdList = new ArrayList<>();
			while (rs.next()) {
				addressIdList.add(rs.getLong("ADDRESS_ID"));
			}

			Random random = new Random(System.currentTimeMillis());
			int offset  = random.nextInt(addressIdList.size());

			Long selectedAddressId = addressIdList.get(offset);

			logger.info(">>count={}, offset={}, selectedAddressId={}", 
					addressIdList.size(), offset, selectedAddressId);	

			sql = "insert into " + config.sourceTableAddress 
					+ " (select * from " + ADDRESS_SRC 
					+ " where address_id = " + selectedAddressId
					+ ")";

			pstmt = sourceConn.prepareStatement(sql);
			pstmt.executeUpdate();	
			pstmt.close();
			sourceConn.commit();

			logger.info(">>> insert sql={}", sql);

			// check data insert oracle
			sql = "select * from " + config.sourceTableAddress + " where address_id = " + selectedAddressId;
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery(sql);
			int i = 0;
			TAddress taddress = new TAddress();
			while (rs.next()) {
				i++;
				taddress.setAddressId(rs.getLong("ADDRESS_ID"));
				taddress.setAddress1(rs.getString("ADDRESS_1"));	
			}
			rs.close();
			pstmt.close();

			if (i != 1) {
				throw new Exception(">>>>> testInsert1Address error, oracle wrong data row count:" + i);
			}
			logger.info(">>> src address ={}", ToStringBuilder.reflectionToString(taddress));


			// check if address id exists in policy_holder, insured_list, contract_bene
			if (isAddressIdExists(sourceConn, config.sourceTablePolicyHolder, taddress.getAddressId())
					|| isAddressIdExists(sourceConn, config.sourceTablePolicyHolder, taddress.getAddressId())
					|| isAddressIdExists(sourceConn, config.sourceTablePolicyHolder, taddress.getAddressId())) {

				// check data insert ignite
				sql = "select * from " + config.sinkTablePartyContact + " where address_id = " + taddress.getAddressId();
				Statement stmt = sinkConn.createStatement();

				List<PartyContact> partyContactList = new ArrayList<>();
				while (true) {
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						PartyContact partyContact2 = new PartyContact();
						partyContact2.setRoleType(rs.getInt("ROLE_TYPE"));
						partyContact2.setListId(rs.getLong("LIST_ID"));
						partyContact2.setPolicyId(rs.getLong("POLICY_ID"));
						partyContact2.setName(rs.getString("NAME"));
						partyContact2.setCertiCode(rs.getString("CERTI_CODE"));
						partyContact2.setMobileTel(rs.getString("MOBILE_TEL"));
						partyContact2.setEmail(rs.getString("EMAIL"));
						partyContact2.setAddressId(rs.getLong("ADDRESS_ID"));
						partyContact2.setAddress1(rs.getString("ADDRESS_1"));

						partyContactList.add(partyContact2);
					}
					if (partyContactList.size() > 0) {
						break;
					} else  {
						logger.info(">>> Wait for 2 seconds");
						Thread.sleep(2000);
					}

				}
				rs.close();
				stmt.close();


			} else {
				// check data insert ignite
				sql = "select * from " + config.sinkTablePartyContactTemp + " where address_id = " + taddress.getAddressId();
				Statement stmt = sinkConn.createStatement();
				List<TAddress> addressList = new ArrayList<TAddress>();
				while (true) {
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						TAddress address = new TAddress();
						address.setAddressId(rs.getLong("ADDRESS_ID"));
						address.setAddress1(rs.getString("ADDRESS_1"));

						addressList.add(address);
					}
					if (addressList.size() > 0) {
						break;
					} else  {
						logger.info(">>> Wait for 2 seconds");
						Thread.sleep(2000);
					}

				}
				rs.close();
				stmt.close();

				if (addressList.size() > 1) {
					throw new Exception(">>> Error Have multiple addresses!!! ");
				}
			}

			logger.info(">>>>> End -> TEST testInsert1Address,   [ OK ]");


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

	private boolean isAddressIdExists(Connection conn, String tableName, long addressId) throws SQLException {
		String sql = "select * from " + tableName + " where address_id = " + addressId;

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				return true;	
			}
		} finally {
			if (rs != null) rs.close();
			if (stmt != null) stmt.close();
		}
		return false;
	}
	private void testInsert1AddressMatchPartyWithoutAddress() throws Exception {
		logger.info(">>>>> START -> testInsert1AddressMatchPartyWithoutAddress");
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

			// from ignite, select 1 partycontact without address
			// partycontact_a = select * from t_party_contact where address_1 is null limit 1;
			sql = "select * from " + config.sinkTablePartyContact + " where address_1 is null";
			pstmt = sinkConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			PartyContact partyContact = null;
			while (rs.next()) {
				partyContact = new PartyContact();
				partyContact.setAddress1(rs.getString("ADDRESS_1"));
				partyContact.setAddressId(rs.getLong("ADDRESS_ID"));
				partyContact.setCertiCode(rs.getString("CERTI_CODE"));
				partyContact.setEmail(rs.getString("EMAIL"));
				partyContact.setListId(rs.getLong("LIST_ID"));
				partyContact.setMobileTel(rs.getString("MOBILE_TEL"));
				partyContact.setName(rs.getString("NAME"));
				partyContact.setPolicyId(rs.getLong("POLICY_ID"));
				partyContact.setRoleType(rs.getInt("ROLE_TYPE"));

				break;
			}
			rs.close();
			pstmt.close();

			if (partyContact == null) {
				throw new Exception("No partycontact without address");
			}
			String addressBefore = partyContact.getAddress1();
			logger.info(">>> selected party without address_1, partyContact={}", ToStringBuilder.reflectionToString(partyContact));


			// select address with address _id
			// select address_1 from t_adress where address_id = ?
			sql = "select * from " + ADDRESS_SRC + " where address_id = ? and address_1 is not null";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, partyContact.getAddressId());
			rs = pstmt.executeQuery();
			TAddress address = null;
			while (rs.next()) {
				address = new TAddress();
				address.setAddressId(rs.getLong("ADDRESS_ID"));
				address.setAddress1(rs.getNString("ADDRESS_1"));
			}
			rs.close();
			pstmt.close();

			if (address == null) {
				throw new Exception("No address with address id=" + partyContact.getAddressId());
			}
			logger.info(">>> selected address ={}", ToStringBuilder.reflectionToString(address));

			// insert address into ignite
			sql = "insert into " + config.sourceTableAddress 
					+ " (select * from " + ADDRESS_SRC 
					+ " where address_id=?)";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());
			pstmt.executeUpdate();
			pstmt.close();
			sourceConn.commit();

			// varify
			sql = "select * from " + config.sinkTablePartyContact + " where address_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());

			logger.info(">>> sql={}", sql);

			boolean address1Updated = false;
			String addressAfter = "";
			while (true) {
				rs = pstmt.executeQuery();
				while (rs.next()) {
					addressAfter = rs.getString("ADDRESS_1");
					if (StringUtils.equals(address.getAddress1(), rs.getString("ADDRESS_1"))) {
						address1Updated = true;
					}
				}
				if (address1Updated) {
					break;
				} else  {
					logger.info(">>> Wait for 2 seconds");
					Thread.sleep(2000);
				}

			}
			rs.close();
			pstmt.close();
			logger.info(">>>>> address1 before={}, after = {}", addressBefore, addressAfter);

			// check t_party_contact_temp has no record for that addres id
			sql = "select * from " + config.sinkTablePartyContactTemp + " where address_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());
			rs = pstmt.executeQuery();

			logger.info(">>>>> sql={}, address_id={}", sql, address.getAddressId());
			while (rs.next()) {
				throw new Exception("t_party_contact_temp found recrd for address id=" + address.getAddressId() + ", which is incorrect.");
			}

			logger.info(">>>>> End testInsert1AddressMatchPartyWithoutAddress,   [ OK ]");
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
	private void testInsert1PartyMatchAddressInTemp() throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>>> START -> testInsert1PartyMatchAddressInTemp");
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

			// select address in temp
			sql = "select * from " + config.sinkTablePartyContactTemp;
			pstmt = sinkConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			Long addressIdInTemp = null;
			String address1 = "";
			while (rs.next()) {
				addressIdInTemp = rs.getLong("ADDRESS_ID");
				address1 = rs.getString("ADDRESS_1");
				break;
			}
			rs.close();
			pstmt.close();

			if (addressIdInTemp == null) {
				throw new Exception("No addressIdInTemp found");
			}
			logger.info(">>> selected addressIdInTemp={}, address1", addressIdInTemp, address1);

			// make sure no part exists in ignite
			sql = "select * from " + config.sinkTablePartyContact + " where address_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, addressIdInTemp);
			rs = pstmt.executeQuery();
			int count = 0;
			while (rs.next()) {
				count++;
				break;
			}
			rs.close();
			pstmt.close();

			if (count > 0) {
				throw new Exception("address id exists both in PartContact and PartyContactTemp");
			}
			logger.info(">>> no partcontact exists for address id=" + addressIdInTemp);


			// select from 
			String partyTableSrc = POLICY_HOLDER_SRC;
			sql = "select * from " + POLICY_HOLDER_SRC + " where address_id = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, addressIdInTemp);
			rs = pstmt.executeQuery();
			Long listId = null;
			String email = null;
			while (rs.next()) {
				listId = rs.getLong("LIST_ID");
				email = rs.getString("EMAIL");
				break;
			}
			rs.close();
			pstmt.close();

			if (listId == null) {
				logger.info("No party found for policyholder for address={}, continue to look up insuredlist",addressIdInTemp);
				partyTableSrc = INSURED_LIST_SRC;
				sql = "select * from " + INSURED_LIST_SRC + " where address_id = ?";
				pstmt = sourceConn.prepareStatement(sql);
				pstmt.setLong(1, addressIdInTemp);
				rs = pstmt.executeQuery();
				listId = null;
				while (rs.next()) {
					listId = rs.getLong("LIST_ID");
					email = rs.getString("EMAIL");
					break;
				}
				rs.close();
				pstmt.close();

				if (listId == null) {
					logger.info("No party found for insured list for address={}, continue to look up contractbene",addressIdInTemp);
					partyTableSrc = CONTRACT_BENE_SRC;
					sql = "select * from " + CONTRACT_BENE_SRC + " where address_id = ?";
					pstmt = sourceConn.prepareStatement(sql);
					pstmt.setLong(1, addressIdInTemp);
					rs = pstmt.executeQuery();
					listId = null;
					while (rs.next()) {
						listId = rs.getLong("LIST_ID");
						email = rs.getString("EMAIL");
						break;
					}
					rs.close();
					pstmt.close();

					if (listId == null) {
						throw new Exception(" no party foud for policyholder, insuredlist, and contractbene");
					}
				}	
			}

			// insert into policy holder
			String table = "";
			if (POLICY_HOLDER_SRC.equals(partyTableSrc)) {
				table = config.sourceTablePolicyHolder;
			} else if (INSURED_LIST_SRC.equals(partyTableSrc)) {
				table = config.sourceTableInsuredList;
			} else if (CONTRACT_BENE_SRC.equals(partyTableSrc)) {
				table = config.sourceTableContractBene;
			}
			sql = "insert into " + table
					+ " (select * from " + partyTableSrc + " where list_id = ?)";

			logger.info(">>> get table={}, partyTableSrc={}, list id={}, email={}", table, partyTableSrc, listId, email);


			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, listId);
			pstmt.executeUpdate();
			pstmt.close();
			sourceConn.commit();

			// verify
			sql = "select * from " + config.sinkTablePartyContactTemp +" where address_id=?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, addressIdInTemp);
			rs = pstmt.executeQuery();
			boolean found = false;
			while (rs.next()) {
				found = true;
				break;
			}
			rs.close();
			pstmt.close();

			if (!found) {
				throw new Exception("Error: record exists in partycontactTemp with address id=" + addressIdInTemp);
			}
			logger.info(">>> record removed in partycontactTemp with address id=" + addressIdInTemp);

			// check part inserted
			sql = "select * from " + config.sinkTablePartyContact + " where list_id = ?";
			logger.info(">>> sql={}, list id={}", sql, listId);
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, listId);

			PartyContact partyContact2 = null;
			while (true) {
				rs = pstmt.executeQuery();
				while (rs.next()) {
					partyContact2 = new PartyContact();
					partyContact2.setAddress1(rs.getString("ADDRESS_1"));
					partyContact2.setAddressId(rs.getLong("ADDRESS_ID"));
					partyContact2.setCertiCode(rs.getString("CERTI_CODE"));
					partyContact2.setEmail(rs.getString("EMAIL"));
					partyContact2.setListId(rs.getLong("LIST_ID"));
					partyContact2.setMobileTel(rs.getString("MOBILE_TEL"));
					partyContact2.setName(rs.getString("NAME"));
					partyContact2.setPolicyId(rs.getLong("POLICY_ID"));
					partyContact2.setRoleType(rs.getInt("ROLE_TYPE"));
				}
				if (partyContact2 != null) {
					break;
				} else  {
					logger.info(">>> Wait for 2 seconds");
					Thread.sleep(2000);
				}
			}
			rs.close();
			pstmt.close();

			if (partyContact2 == null) {
				throw new Exception("partyContact2 is null");
			}
			if (partyContact2.getAddressId().longValue() != addressIdInTemp.longValue()) {
				throw new Exception("address id does not equal");
			}

			// get from source table
			sql = "select * from " + table + " where list_id = ?";
			logger.info(">>> sql3={}, list id={}", sql, listId);
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, listId);
			rs = pstmt.executeQuery();
			PartyContact partyContact3 = null;
			while (rs.next()) {
				partyContact3 = new PartyContact();
				partyContact3.setAddress1(partyContact2.getAddress1());
				partyContact3.setAddressId(rs.getLong("ADDRESS_ID"));
				partyContact3.setCertiCode(rs.getString("CERTI_CODE"));
				if (CONTRACT_BENE_ROLE_TYPE == partyContact2.getRoleType().intValue()) {
					partyContact3.setEmail(null);
				} else {	
					partyContact3.setEmail(rs.getString("EMAIL"));
				}
				partyContact3.setListId(rs.getLong("LIST_ID"));
				partyContact3.setMobileTel(rs.getString("MOBILE_TEL"));
				partyContact3.setName(rs.getString("NAME"));
				partyContact3.setPolicyId(rs.getLong("POLICY_ID"));
				partyContact3.setRoleType(partyContact2.getRoleType());
			}
			rs.close();
			pstmt.close();

			if (partyContact3 == null) {
				throw new Exception("partyContact3 is null");
			}
			if (partyContact3.getAddressId().longValue() != addressIdInTemp.longValue()) {
				logger.info("partyContact2={}", ToStringBuilder.reflectionToString(partyContact2));
				logger.info("partyContact3={}", ToStringBuilder.reflectionToString(partyContact3));
				throw new Exception("address id does not equal");
			}

			if (!partyContact2.equals(partyContact3)) {
				logger.info("partyContact2={}", ToStringBuilder.reflectionToString(partyContact2));
				logger.info("partyContact3={}", ToStringBuilder.reflectionToString(partyContact3));
				throw new Exception("partycontact does not equal");
			}

			logger.info("partyContact2={}", ToStringBuilder.reflectionToString(partyContact2));
			logger.info("partyContact3={}", ToStringBuilder.reflectionToString(partyContact3));
			logger.info(">>>>> START -> testInsert1PartyMatchAddressInTemp     [  OK  ]");

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
	private void testInsert1PartyMatchPartyWithAddress1() throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>>> START -> testInsert1PartyMatchPartyWithAddress1");
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

			/*
			 * select c.list_id, d.list_id, c.address_id, c.address_1
from 
(
select a.list_id, a.address_id, b.address_1 
from test_t_policy_holder1 a
inner join test_t_address1 b on a.address_id = b.address_id 
) c
inner join
(
select a.list_id, a.address_id, b.address_1 
from test_t_insured_list1 a
inner join test_t_address1 b on a.address_id = b.address_id
) d
on c.address_id = d.address_id;

			 */
			// policy_holder, list_id = 2668, address_id = 2907063
			// policy_holder, list_id = 1875, address_id = 2907063
			// insured_list, list_id = 12829474, address_id = 2907063
			// insured_list, list_id = 12829474, address_id = 2907063

			long selectedAddressId = 2907063;
			long selectedPhListId1 = 2668;
			long selectedIlListId1 = 12829474;
			long selectedPhListId2 = 1875;

			// make sure party contact does not exists with above list_id
			sql = "select * from " + config.sinkTablePartyContact 
					+ " where address_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, selectedAddressId);
			rs = pstmt.executeQuery();
			boolean found = false;
			while (rs.next()) {
				found = true;
			}
			rs.close();
			pstmt.close();

			if (found) {
				throw new Exception("party contact exists, address id=" + selectedAddressId);
			}

			// insert party policy_holder, list_id = 2668
			sql = "insert into " + config.sourceTablePolicyHolder
					+ " (select * from " + POLICY_HOLDER_SRC 
					+ " where list_id = ?)";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, selectedPhListId1);
			pstmt.executeUpdate();
			pstmt.close();

			// insert address address_id = 2907063
			sql = "insert into " + config.sourceTableAddress
					+ " (select * from " + ADDRESS_SRC 
					+ " where address_id = ?)";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, selectedAddressId);
			pstmt.executeUpdate();
			pstmt.close();

			sourceConn.commit();

			// insert insured_list, list_id = 12829474
			sql = "insert into " + config.sourceTableInsuredList
					+ " (select * from " + INSURED_LIST_SRC 
					+ " where list_id = ?)";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, selectedIlListId1);
			pstmt.executeUpdate();
			pstmt.close();


			// insert policy_holder, list_id = 1875,
			sql = "insert into " + config.sourceTablePolicyHolder
					+ " (select * from " + POLICY_HOLDER_SRC 
					+ " where list_id = ?)";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, selectedPhListId2);
			pstmt.executeUpdate();
			pstmt.close();

			sourceConn.commit();

			// verify
			// get address info
			sql = "select * from " + config.sourceTableAddress
					+ " where address_id = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, selectedAddressId);
			rs = pstmt.executeQuery();
			String selectedAddress1 = null;
			while (rs.next()) {
				selectedAddress1 = rs.getString("ADDRESS_1");
			}
			pstmt.close();

			// check partycontact
			sql = "select * from " + config.sinkTablePartyContact
					+ " where address_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, selectedAddressId);
			rs = pstmt.executeQuery();
			List<PartyContact> contactList = new ArrayList<>();
			while (rs.next()) {
				PartyContact partyContact = new PartyContact();
				partyContact.setAddress1(rs.getString("ADDRESS_1"));
				partyContact.setAddressId(rs.getLong("ADDRESS_ID"));
				partyContact.setCertiCode(rs.getString("CERTI_CODE"));
				partyContact.setEmail(rs.getString("EMAIL"));
				partyContact.setListId(rs.getLong("LIST_ID"));
				partyContact.setMobileTel(rs.getString("MOBILE_TEL"));
				partyContact.setName(rs.getString("NAME"));
				partyContact.setPolicyId(rs.getLong("POLICY_ID"));
				partyContact.setRoleType(rs.getInt("ROLE_TYPE"));
				contactList.add(partyContact);
			}
			rs.close();
			pstmt.close();

			for (PartyContact contact : contactList) {
				if (selectedPhListId1 == contact.getListId() || selectedPhListId2 == contact.getListId()) {
					if (POLICY_HOLDER_ROLE_TYPE != contact.getRoleType().intValue()) {
						throw new Exception(">>> roletype=" + contact.getRoleType().intValue() 
								+ ",list id incorrect,listid=" + contact.getListId());
					} 
				} else if (selectedIlListId1 == contact.getListId()) {
					if (INSURED_LIST_ROLE_TYPE != contact.getRoleType().intValue()) {
						throw new Exception(">>> roletype=" + contact.getRoleType().intValue() 
								+ ",list id incorrect,listid=" + contact.getListId());
					} 
				}
				// check equal
				PartyContact sourcePartyContact = getSourcePartyContactBy(sourceConn, contact.getRoleType(), contact.getListId(), contact.getAddress1());
				if (contact.equals(sourcePartyContact)) {
					logger.info(">>> party contact equal, list id={}", contact.getListId());
				} else {
					logger.info(">>> contact={}", ToStringBuilder.reflectionToString(contact));
					logger.info(">>> sourcePartyContact={}", ToStringBuilder.reflectionToString(sourcePartyContact));
					throw new Exception(">>> party contact NOT equal");
				}

				if (contact.getAddressId().longValue() != selectedAddressId) {
					throw new Exception(">>> selectedAddressId=" + selectedAddressId
							+ ",contact address id=" + contact.getAddressId());
				}
				if (!StringUtils.equals(selectedAddress1, contact.getAddress1())) {
					throw new Exception(">>> selectedAddress1=" + selectedAddress1
							+ ",contact getAddress1 =" + contact.getAddress1());
				}
			}
			logger.info(">>>>> END -> testInsert1PartyMatchPartyWithAddress1     [  OK  ]");

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

	private PartyContact getSourcePartyContactBy(Connection conn, int roleType, long listId, String address1) throws SQLException {
		String table = "";
		if (POLICY_HOLDER_ROLE_TYPE == roleType) {
			table = config.sourceTablePolicyHolder;
		} else if (INSURED_LIST_ROLE_TYPE == roleType) {
			table = config.sourceTableInsuredList;
		} else if (CONTRACT_BENE_ROLE_TYPE == roleType) {
			table = config.sourceTableContractBene;
		}

		String sql = "select * from " + table
				+ " where list_id = ?";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, listId);
			rs = pstmt.executeQuery();
			PartyContact partyContact = new PartyContact();
			while (rs.next()) {	
				partyContact.setAddress1(address1);
				partyContact.setAddressId(rs.getLong("ADDRESS_ID"));
				partyContact.setCertiCode(rs.getString("CERTI_CODE"));
				partyContact.setEmail(rs.getString("EMAIL"));
				partyContact.setListId(rs.getLong("LIST_ID"));
				partyContact.setMobileTel(rs.getString("MOBILE_TEL"));
				partyContact.setName(rs.getString("NAME"));
				partyContact.setPolicyId(rs.getLong("POLICY_ID"));
				partyContact.setRoleType(roleType);
			}
			return partyContact;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}
	private void updateContactForPartyContact(int roleType, String updateField, String updateValue) throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>>> START -> updateContactForPartyContact");
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

			// select party contact with role type
			sql = "select * from t_party_contact where role_type = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setInt(1, roleType);
			rs = pstmt.executeQuery();
			List<Long> selectedListIdList = new ArrayList<>();
			while (rs.next()) {
				selectedListIdList.add(rs.getLong("LIST_ID"));
			}
			rs.close();
			pstmt.close();

			if (selectedListIdList.size() == 0) {
				throw new Exception("no party contact record for role type=" + roleType);
			}
			Random random = new Random(System.currentTimeMillis());
			int offset  = random.nextInt(selectedListIdList.size());
			Long selectListId = selectedListIdList.get(offset);

			// select from source table
			String table = null;
			if (POLICY_HOLDER_ROLE_TYPE == roleType) {
				table = config.sourceTablePolicyHolder;
			} else if (INSURED_LIST_ROLE_TYPE == roleType) {
				table = config.sourceTableInsuredList;
			} else if (CONTRACT_BENE_ROLE_TYPE == roleType) {
				table = config.sourceTableContractBene;
			}
			sql = "";
			if (StringUtils.equals("EMAIL", updateField)) {
				sql = "update " + table + " set EMAIL = ? where list_id = ?";	
			} else if (StringUtils.equals("MOBILE_TEL", updateField)) {
				sql = "update " + table + " set MOBILE_TEL = ? where list_id = ?";	
			} 
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setString(1, updateValue);
			pstmt.setLong(2, selectListId);
			pstmt.executeUpdate();
			pstmt.close();
			sourceConn.commit();

			// verify
			sql = "select * from " + config.sinkTablePartyContact + " where role_type=? and list_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setInt(1, roleType);
			pstmt.setLong(2, selectListId);
			boolean found = false;
			String queryValue = null;
			while (true) {
				rs = pstmt.executeQuery();
				while (rs.next()) {
					if (StringUtils.equals("EMAIL", updateField)) {
						queryValue = rs.getString("EMAIL");	
					} else if (StringUtils.equals("MOBILE_TEL", updateField)) {
						queryValue = rs.getString("MOBILE_TEL");	
					} 
					if (!StringUtils.equals(updateValue, queryValue)) {
						logger.error(">>> no match updateAddress1={}, queryValue={}", updateValue, queryValue);

						break;
					}
					found = true;
				}
				if (found) {
					logger.info(">>> update value update match!!");
					break;
				} else  {
					logger.info(">>> Wait for 2 seconds");
					Thread.sleep(2000);
				}
			}
			rs.close();
			pstmt.close();


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
		logger.info(">>>>>>>>>>>>>>>>>>>>> END -> updateContactForPartyContact");
	}
	private void updateAddress1ForPartyContact() throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>>> START -> updateAddress1ForPartyContact");
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

			// update partycontact address
			sql = "select * from " + config.sinkTablePartyContact+ " where address_1 is not null";
			pstmt = sinkConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			String address1 = null;
			Long addressId = null;
			while (rs.next()) {
				address1 = rs.getString("ADDRESS_1");
				addressId = rs.getLong("ADDRESS_ID");
				break;
			}
			rs.close();
			pstmt.close();
			if (addressId == null) {
				throw new Exception("Record not found for address1 is null");
			}

			// select from source table
			sql = "select * from " + config.sourceTableAddress + " where address_id = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, addressId);
			rs = pstmt.executeQuery();
			String srcAddress1 = null;
			Long srcAddressId = null;
			while (rs.next()) {
				srcAddress1 = rs.getString("ADDRESS_1");
				srcAddressId = rs.getLong("ADDRESS_ID");
				break;
			}
			rs.close();
			pstmt.close();

			if (srcAddressId == null) {
				throw new Exception("Record not found for addressid :" + addressId);
			}

			// update address
			String updateAddress1 = "新竹市長春街167像";
			sql = "update " + config.sourceTableAddress 
					+ " set address_1 = ? where address_id =?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setString(1, updateAddress1);
			pstmt.setLong(2, addressId);
			pstmt.executeUpdate();
			pstmt.close();

			sourceConn.commit();

			// verify
			sql = "select * from " + config.sinkTablePartyContact + " where address_id = ?";
			logger.info(">>> sql={}, addressid={}", sql, addressId);
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, addressId);

			boolean found = false;
			while (true) {
				rs = pstmt.executeQuery();
				while (rs.next()) {
					if (!StringUtils.equals(updateAddress1, rs.getString("ADDRESS_1"))) {
						
						break;
					}
					found = true;
				}
				if (found) {
					logger.info(">>> address1 update match!!");
					break;
				} else  {
					logger.info(">>> Wait for 2 seconds");
					Thread.sleep(2000);
				}
			}
			rs.close();
			pstmt.close();


			logger.info(">>>>> END -> updateAddress1ForPartyContact     [  OK  ]");

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
	private void insertParty(Connection conn, Integer roleType, Long listId) throws SQLException {
		String sql;
		PreparedStatement pstmt = null;
		try {
			String srcTable = "";
			String srcTable1 = "";
			if (POLICY_HOLDER_ROLE_TYPE == roleType.intValue()) {
				srcTable = config.sourceTablePolicyHolder;
				srcTable1 = POLICY_HOLDER_SRC;
			} else if (INSURED_LIST_ROLE_TYPE == roleType.intValue()) {
				srcTable = config.sourceTableInsuredList;
				srcTable1 = INSURED_LIST_SRC;
			} else if (CONTRACT_BENE_ROLE_TYPE == roleType.intValue()) {
				srcTable = config.sourceTableContractBene;
				srcTable1 = CONTRACT_BENE_SRC;
			}
			sql = "insert into " + srcTable
					+ " select * from " + srcTable1 + " where list_id = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, listId);
			pstmt.executeUpdate();
			pstmt.close();

			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		} finally {
			if (pstmt != null) pstmt.close();
		}

	}
	private void insertAddress(Connection conn, Long addressId) throws SQLException {
		String sql;
		PreparedStatement pstmt = null;
		try {

			sql = "insert into " + config.sourceTableAddress
					+ " select * from " + ADDRESS_SRC + " where address_id = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, addressId);
			pstmt.executeUpdate();
			pstmt.close();

			conn.commit();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}

}
