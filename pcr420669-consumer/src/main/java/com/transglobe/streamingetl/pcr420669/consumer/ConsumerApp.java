package com.transglobe.streamingetl.pcr420669.consumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.consumer.model.Address;
import com.transglobe.streamingetl.pcr420669.consumer.model.ContractBene;
import com.transglobe.streamingetl.pcr420669.consumer.model.InsuredList;
import com.transglobe.streamingetl.pcr420669.consumer.model.PartyContact;
import com.transglobe.streamingetl.pcr420669.consumer.model.PolicyHolder;


public class ConsumerApp {

	static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	private static final Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static final Integer INSURED_LIST_ROLE_TYPE = 2;
	private static final Integer CONTRACT_BENE_ROLE_TYPE = 3;

	private Config config;

	private BasicDataSource connPool;

	public ConsumerApp(String fileName) throws Exception {
		logger.info(">>>>>config fileName={}", fileName);
		config = Config.getConfig(fileName);

		connPool = new BasicDataSource();
		connPool.setUrl(config.sinkDbUrl);
		connPool.setUsername(null);
		connPool.setPassword(null);
		connPool.setDriverClassName(config.sinkDbDriver);
		connPool.setMaxTotal(5);
	}
	public static void main(String[] args) {

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);

		ConsumerApp app = null;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new ConsumerApp(configFile);

			app.run();

			app.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			if (app != null) app.close();

		}
	}
	private void run() throws Exception {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", config.bootstrapServers);
		props.setProperty("group.id", config.groupId);
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("max.poll.interval.ms", "120000");
		props.setProperty("session.timeout.ms", "300000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = null;
		try {

			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(config.topicList);

			while (true) {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				if (!records.isEmpty()) {
					int count = 0;
					Connection conn = connPool.getConnection();
					try {
						for (ConsumerRecord<String, String> record : records) {
							logger.info(">>>Topic: {}, Partition: {}, Offset: {}, key: {}, value: {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
							ObjectMapper objectMapper = new ObjectMapper();
							objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

							JsonNode jsonNode = objectMapper.readTree(record.value());
							JsonNode payload = jsonNode.get("payload");
							count++;
							logger.info("   >>>count={}", count);
							try {
								//	logger.info("   >>>record.value()={}", record.value());

								String operation = payload.get("OPERATION").asText();
								logger.info("   >>>operation={}", operation);
								if ("INSERT".equals(operation)) {
									logger.info("   >>>doInsert");
									doInsert(conn, objectMapper, payload);

								} else if ("UPDATE".equals(operation)) {
									logger.info("   >>>doUpdate");
									doUpdate(conn, objectMapper, payload);

								} else if ("DELETE".equals(operation)) {
									logger.info("   >>>doDelete");
									doDelete(conn, objectMapper, payload);

								}

							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								logger.error(e.getMessage(), e);

							} finally {

							}

						} 
						//					if (pstmt != null) pstmt.close();
						//					
						//					if (pstmt2 != null) pstmt2.close();
						//					
						//					conn.commit();  
						//					
						//					conn.close();  
					} finally {
						if (conn != null) conn.close();
					}
					consumer.commitSync();
				}

			}
		} catch (Exception e) {
			throw new Exception(e.getMessage(), e);
		} catch (Throwable e) {
			throw new Exception(e.getMessage(), e);
		} finally {
			try {
				consumer.commitSync(); 
			} finally {
				consumer.close();
			}
		}
	}

	private void createTopics() throws InterruptedException, ExecutionException {

		String bootstrapServers = config.bootstrapServers;
		List<String> topicList = config.topicList;

		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		AdminClient admin = AdminClient.create(config);

		Set<String> existingTopics = admin.listTopics().names().get();
		//listing
		admin.listTopics().names().get().forEach(System.out::println);

		//creating new topic
		for (String topic : topicList) {
			if (!existingTopics.contains(topic)) {
				NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
				admin.createTopics(Collections.singleton(newTopic));
				logger.info(">>> topic={} created", topic);
			}
		}	
	}
	private void close() {
		try {
			if (connPool != null) connPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
	private void doInsert(Connection conn, ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String fullTableName = payload.get("SEG_OWNER").asText() + "." + payload.get("TABLE_NAME").asText();
		logger.info(">>> fulltableName={}", fullTableName);
		String data = payload.get("data").toString();

		if (config.sourceTablePolicyHolder.equals(fullTableName)
				|| config.sourceTableInsuredList.equals(fullTableName)
				|| config.sourceTableContractBene.equals(fullTableName)) {
			logger.info("   >>>insert partyContact");
			PartyContact partyContact = objectMapper.readValue(data, PartyContact.class);
			logger.info(">>> partyContact={}", partyContact);

			if (config.sourceTablePolicyHolder.equals(fullTableName)) {
				partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
			} else if (config.sourceTableInsuredList.equals(fullTableName)) {
				partyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
			} else if (config.sourceTableContractBene.equals(fullTableName)) {
				partyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
				partyContact.setEmail(null); // 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
			}
			logger.info(">>> start insertPartyContact");
			insertPartyContact(conn, partyContact);

		} else if (config.sourceTableAddress.equals(fullTableName)) {
			Address address = objectMapper.readValue(data, Address.class);
			logger.info(">>> address={}", address);
			logger.info("   >>>insert Address");
			insertAddress(conn, address);
		}


	}
	private void doDelete(Connection conn, ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String fullTableName = payload.get("SEG_OWNER").asText() + "." + payload.get("TABLE_NAME").asText();
		logger.info(">>> fullTableName tableName={}", fullTableName);
		String before = payload.get("before").toString();
		String sql = "";
		Integer roleType = null;
		Long listId = null;

		if (config.sourceTablePolicyHolder.equals(fullTableName)
				|| config.sourceTableInsuredList.equals(fullTableName)
				|| config.sourceTableContractBene.equals(fullTableName)) {
			PartyContact beforePartyContact = objectMapper.readValue(before, PartyContact.class);
			logger.info(">>> beforePartyContact={}", beforePartyContact);

			if (config.sourceTablePolicyHolder.equals(fullTableName)) {
				beforePartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
			} else if (config.sourceTableInsuredList.equals(fullTableName)) {
				beforePartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
			} else if (config.sourceTableContractBene.equals(fullTableName)) {
				beforePartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
				beforePartyContact.setEmail(null); // 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
			}

			throw new Exception(">>> Delete action is not supported");

		} else if (config.sourceTableAddress.equals(fullTableName)) {
			Address beforeAddress = objectMapper.readValue(before, Address.class);
			logger.info(">>> beforeAddress={}", beforeAddress);

			throw new Exception(">>> Delete action for address is not supported");
		}


		sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact + " where role_type = ? and list_id = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, roleType);
		pstmt.setLong(2, listId);
		ResultSet resultSet = pstmt.executeQuery();
		Integer count = 0; 
		while (resultSet.next()) {
			count = resultSet.getInt("COUNT");
		}
		logger.info(">>> count={}", count);
		resultSet.close();
		pstmt.close();

		if (count > 0) {
			sql = "delete " + config.sinkTablePartyContact + " where role_type = ? and list_id = ?"; 
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, roleType);
			pstmt.setLong(2, listId);

			pstmt.executeUpdate();
		} else {
			// record exists, error
			String error = String.format("table=%s record does not exist, role_type=%d, list_id=%d", config.sinkTablePartyContact, roleType, listId);
			throw new Exception(error);
		}
		pstmt.close();

	}
	private void doUpdate(Connection conn, ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String fullTableName = payload.get("SEG_OWNER").asText() + "." + payload.get("TABLE_NAME").asText();
		logger.info(">>> fulltableName={}", fullTableName);
		String data = payload.get("data").toString();
		String before = payload.get("before").toString();
		String sql = "";

		if (config.sourceTablePolicyHolder.equals(fullTableName)
				|| config.sourceTableInsuredList.equals(fullTableName)
				|| config.sourceTableContractBene.equals(fullTableName)) {
			PartyContact oldpartyContact = objectMapper.readValue(before, PartyContact.class);
			PartyContact newpartyContact = objectMapper.readValue(data, PartyContact.class);
			logger.info(">>> oldpartyContact={}", oldpartyContact);
			logger.info(">>> newpartyContact={}", newpartyContact);

			if (config.sourceTablePolicyHolder.equals(fullTableName)) {
				oldpartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
				newpartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
			} else if (config.sourceTableInsuredList.equals(fullTableName)) {
				oldpartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
				newpartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
			} else if (config.sourceTableContractBene.equals(fullTableName)) {
				oldpartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
				newpartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
				newpartyContact.setEmail(null); // 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
			}

			updatePartyContact(conn, newpartyContact, newpartyContact);

		} else if (config.sourceTableAddress.equals(fullTableName)) {
			Address oldAddress = objectMapper.readValue(before, Address.class);
			Address newAddress = objectMapper.readValue(data, Address.class);
			logger.info(">>> oldAddress={}", oldAddress);
			logger.info(">>> newAddress={}", newAddress);

			updateAddress(conn, oldAddress, newAddress);
		}

	}

	private String getAddress1FromPartyContact(Connection conn, Long addressId) throws SQLException {

		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		String address1 = null;
		try {
			String sql = "select ADDRESS_1 from " + config.sinkTablePartyContact + " where address_id = " + addressId;
			pstmt = conn.prepareStatement(sql);
			resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				address1 = resultSet.getString("ADDRESS_1");
				break;
			}
			resultSet.close();
			pstmt.close();

		} finally {
			if (resultSet != null) resultSet.close();
			if (pstmt != null) pstmt.close();
		}

		return address1;
	}
	private void deletePartyContactTemp(Connection conn, Long addressId) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "delete " + config.sinkTablePartyContactTemp + " where address_id = " + addressId;
			pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();

			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private String getAddress1FromPartyContactTemp(Connection conn, Long addressId) throws SQLException {
		logger.info(">>> ccc");
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		String address1 = null;
		try {
			String sql = "select ADDRESS_1 from " + config.sinkTablePartyContactTemp + " where address_id = " + addressId;
			logger.info(">>> sql={}", sql);
			pstmt = conn.prepareStatement(sql);
			resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				address1 = resultSet.getString("ADDRESS_1");
				break;
			}
			logger.info(">>> address1={}", address1);

			resultSet.close();
			pstmt.close();

		} finally {
			if (resultSet != null) resultSet.close();
			if (pstmt != null) pstmt.close();
		}

		return address1;
	}
	private Integer getCount(Connection conn, String sql) throws SQLException {

		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet resultSet = pstmt.executeQuery();
		Integer count = 0; 
		while (resultSet.next()) {
			count = resultSet.getInt("COUNT");
		}
		logger.info(">>> count={}", count);
		resultSet.close();
		pstmt.close();

		return count;
	}
	private void updatePartyContact(Connection conn, PartyContact oldPartyContact, PartyContact newPartyContact) throws Exception  {
		String sql = "";
		PreparedStatement pstmt = null;

		try {
			// check if address id is changed
			if (oldPartyContact.getAddressId().longValue() == newPartyContact.getAddressId().longValue()) {
				// address id has not changed
				sql = "update " + config.sinkTablePartyContact 
						+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?" 
						+ " where ROLE_TYPE = ? and LIST_ID = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setLong(1, newPartyContact.getPolicyId());
				pstmt.setString(2, newPartyContact.getName());
				pstmt.setString(3, newPartyContact.getCertiCode());
				pstmt.setString(4, newPartyContact.getMobileTel());
				pstmt.setString(5, newPartyContact.getEmail());
				pstmt.setInt(6, newPartyContact.getRoleType());
				pstmt.setLong(7, newPartyContact.getListId());

				pstmt.executeUpdate();
			} else {
				// address id has changed
				String address = getAddress1FromPartyContact(conn, newPartyContact.getAddressId());
				if (StringUtils.isBlank(address)) {
					String address2 = getAddress1FromPartyContactTemp(conn, newPartyContact.getAddressId());

					deletePartyContactTemp(conn, newPartyContact.getAddressId());

					sql = "update " + config.sinkTablePartyContact 
							+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=?" 
							+ " where ROLE_TYPE = ? and LIST_ID = ?";
					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, newPartyContact.getPolicyId());
					pstmt.setString(2, newPartyContact.getName());
					pstmt.setString(3, newPartyContact.getCertiCode());
					pstmt.setString(4, newPartyContact.getMobileTel());
					pstmt.setString(5, newPartyContact.getEmail());
					pstmt.setLong(6, newPartyContact.getAddressId());
					pstmt.setString(7, address2);
					pstmt.setInt(8, newPartyContact.getRoleType());
					pstmt.setLong(9, newPartyContact.getListId());

					pstmt.executeUpdate();
				} else {
					sql = "update " + config.sinkTablePartyContact 
							+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=?" 
							+ " where ROLE_TYPE = ? and LIST_ID = ?";
					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, newPartyContact.getPolicyId());
					pstmt.setString(2, newPartyContact.getName());
					pstmt.setString(3, newPartyContact.getCertiCode());
					pstmt.setString(4, newPartyContact.getMobileTel());
					pstmt.setString(5, newPartyContact.getEmail());
					pstmt.setLong(6, newPartyContact.getAddressId());
					pstmt.setString(7, address);
					pstmt.setInt(8, newPartyContact.getRoleType());
					pstmt.setLong(9, newPartyContact.getListId());

					pstmt.executeUpdate();
				}
			}
		} finally {
			if (pstmt != null) pstmt.close();
		}

	}

	private void updateAddress(Connection conn, Address oldAddress, Address newAddress) throws Exception  {

		PreparedStatement pstmt = null;
		String sql = "";
		try {
			// update PartyContact
			sql = "update " + config.sinkTablePartyContact
					+ " set ADDRESS_1 = ? where ADDRESS_ID = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, newAddress.getAddress1());
			pstmt.setLong(2, oldAddress.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

			// update PartyContactTemp
			sql = "update " + config.sinkTablePartyContactTemp
					+ " set ADDRESS_1 = ? where ADDRESS_ID = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, newAddress.getAddress1());
			pstmt.setLong(2, oldAddress.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private void insertPartyContact(Connection conn, PartyContact partyContact) throws Exception  {
		logger.info(">>> start fun:insertPartyContact");
		PreparedStatement pstmt = null;

		String sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact 
				+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
		logger.info(">>> sql={}", sql);
		int count = getCount(conn, sql);
		logger.info(">>> sinkTablePartyContact={}, count={}", config.sinkTablePartyContact, count);
		if (count == 0) {
			sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
					+ " values (?,?,?,?,?,?,?,?,?)";
			logger.info(">>> sql={}", sql);
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, partyContact.getRoleType());
			pstmt.setLong(2, partyContact.getListId());
			pstmt.setLong(3, partyContact.getPolicyId());
			pstmt.setString(4, partyContact.getName());
			pstmt.setString(5, partyContact.getCertiCode());
			pstmt.setString(6, partyContact.getMobileTel());
			pstmt.setString(7, partyContact.getEmail());
			pstmt.setLong(8, partyContact.getAddressId());
			pstmt.setString(9, partyContact.getAddress1());

			pstmt.executeUpdate();
			pstmt.close();

			logger.info(">>> ok1");

			String address1 = getAddress1FromPartyContact(conn, partyContact.getAddressId());
			logger.info(">>> address1={}", address1);

			if (address1 == null) {
				address1 = getAddress1FromPartyContactTemp(conn, partyContact.getAddressId());

				logger.info(">>> aa address1={}", address1);

				// delete PartyContactTemp
				deletePartyContactTemp(conn, partyContact.getAddressId());

				logger.info(">>> bb");
			} 
			// update address1
			sql = "update " + config.sinkTablePartyContact + " set ADDRESS_1 = ? where role_type = ? and list_id = ?";
			logger.info(">>> update={}", sql);

			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, address1);
			pstmt.setInt(2, partyContact.getRoleType());
			pstmt.setLong(3, partyContact.getListId());
			pstmt.executeUpdate();
			pstmt.close();

			logger.info(">>> ok2");
		} else {
			// record exists, error
			String error = String.format("table=%s record already exists, role_type=%d, list_id=%d", config.sinkTablePartyContact, partyContact.getRoleType(), partyContact.getListId());
			throw new Exception(error);
		}
	}
	private void insertAddress(Connection conn, Address address) throws Exception {

		String sql = "select ROLE_TYPE,LIST_ID from " + config.sinkTablePartyContact + " where address_id = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());
			ResultSet resultSet = pstmt.executeQuery();
			int count = 0;
			while (resultSet.next()) {
				count++;
				break;

			}
			resultSet.close();
			pstmt.close();

			if (count == 0) {
				// insert 
				sql = "insert into " + config.sinkTablePartyContactTemp + " (ADDRESS_ID,ADDRESS_1)" + " values (?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setLong(1, address.getAddressId());
				pstmt.setString(2, address.getAddress1());
				pstmt.executeUpdate();
				pstmt.close();

				logger.info(">>> no address exists, insert sql={} ", sql);
			} else {
				// update party contact
				logger.info(">>> update party contact address");
				sql = "update "  + config.sinkTablePartyContact + " set address_1 = ? where address_id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setString(1, address.getAddress1());
				pstmt.setLong(2, address.getAddressId());
				pstmt.executeUpdate();
				pstmt.close();
			}

		} finally {
			if (pstmt != null) pstmt.close();
		}

	}
}
