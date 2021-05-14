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
import com.transglobe.streamingetl.pcr420669.test.model.TAddress;
import com.transglobe.streamingetl.pcr420669.test.model.PartyContact;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;


public class TestApp {
	private static final Logger logger = LoggerFactory.getLogger(TestApp.class);

	private static final String CONFIG_FILE_NAME = "config.dev1.properties";
	private static String BASE_URL = "http://localhost:8080/partycontact/v1.0";

	private static int ADDRESS_ROLE_TYPE = 0;
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
		TestApp app;

		try {
			app = new TestApp(CONFIG_FILE_NAME);

			app.testInit();

			app.testInsert1Party(POLICY_HOLDER_ROLE_TYPE);

			app.testInsert1Party(INSURED_LIST_ROLE_TYPE);

			app.testInsert1Party(CONTRACT_BENE_ROLE_TYPE);

			app.testInsert1Address();

			app.testInsert1AddressMatchPartyWithoutAddress();

			app.testInsert1PartyMatchAddressInTemp();


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

	private void testInsert1Address() throws Exception {
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
			int count1 = 0;
			while (rs.next()) {
				count++;
				break;
			}
			rs.close();
			pstmt.close();

			if (count1 > 0) {
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
}
