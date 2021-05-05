package com.transglobe.streamingetl.pcr420669.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
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

	private static final String POLICY_HOLDER_TABLE_NAME = "T_POLICY_HOLDER";
	private static final String INSURED_LIST_TABLE_NAME = "T_INSURED_LIST";
	private static final String CONTRACT_BENE_TABLE_NAME = "T_CONTACT_BENE";
	private static final String ADDRESS_TABLE_NAME = "T_ADDRESS";
	private static final String PARTY_CONTACT_TABLE_NAME = "T_PARTY_CONTACT";

	private static final Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static final Integer INSURED_LIST_ROLE_TYPE = 2;
	private static final Integer CONTRACT_BENE_ROLE_TYPE = 3;
	private static final Integer ADDRESS_ROLE_TYPE = 0;

	private Config config;

	private BasicDataSource connPool;

	public ConsumerApp(String fileName) throws Exception {
		logger.info(">>>>>config fileName={}", fileName);
		config = Config.getConfig(fileName);

		connPool = new BasicDataSource();
		connPool.setUrl(config.sinkDbUrl);
		connPool.setUsername(config.sinkDbUsername);
		connPool.setPassword(config.sinkDbPassword);
		connPool.setDriverClassName(config.sinkDbDriver);
		connPool.setMaxTotal(2);
	}
	public static void main(String[] args) {
		
		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		
		ConsumerApp app = null;
		try {
			String confileFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;
			app = new ConsumerApp(confileFile);

			app.createTopics();

			app.createTable();

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
					for (ConsumerRecord<String, String> record : records) {
						logger.info(">>>Topic: {}, Partition: {}, Offset: {}, key: {}, value: {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
						ObjectMapper objectMapper = new ObjectMapper();
						objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

						JsonNode jsonNode = objectMapper.readTree(record.value());
						JsonNode payload = jsonNode.get("payload");
						count++;
						logger.info("   >>>count={}", count);
						Connection conn = null;
						try {
							logger.info("   >>>record.value()={}", record.value());

							String operation = payload.get("OPERATION").asText();
							logger.info("   >>>operation={}", operation);
							if ("INSERT".equals(operation)) {
								doInsert(objectMapper, payload);

							} else if ("UPDATE".equals(operation)) {
								doUpdate(objectMapper, payload);

							} else if ("DELETE".equals(operation)) {
								doDelete(objectMapper, payload);

							}

						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							logger.error(e.getMessage(), e);

						} finally {
							if (conn != null) conn.close();
						}

					} 
					//					if (pstmt != null) pstmt.close();
					//					
					//					if (pstmt2 != null) pstmt2.close();
					//					
					//					conn.commit();  
					//					
					//					conn.close();  

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
	private void doInsert(ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String tableName = payload.get("TABLE_NAME").asText();
		logger.info(">>> tableName={}", tableName);
		String data = payload.get("data").toString();

		PartyContact partyContact = objectMapper.readValue(data, PartyContact.class);
		String sql = "";
		if (POLICY_HOLDER_TABLE_NAME.equals(tableName)) {
			partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
		} else if (INSURED_LIST_TABLE_NAME.equals(tableName)) {
			partyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
		} else if (CONTRACT_BENE_TABLE_NAME.equals(tableName)) {
			partyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
		} else if (ADDRESS_TABLE_NAME.equals(tableName)) {
			partyContact.setRoleType(ADDRESS_ROLE_TYPE);
			partyContact.setListId(partyContact.getAddressId());
		}
		logger.info(">>> partyContact={}", partyContact);

		PreparedStatement pstmt = null;
		if (ADDRESS_TABLE_NAME.equals(tableName)) {
			Connection conn = connPool.getConnection();
			sql = "select ROLE_TYPE,LIST_ID from " + PARTY_CONTACT_TABLE_NAME + " where address_id = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, partyContact.getAddressId());
			ResultSet resultSet = pstmt.executeQuery();
			int count = 0;
			while (resultSet.next()) {
				count++;
				Integer roleType = resultSet.getInt("ROLE_TYPE");
				Long listId = resultSet.getLong("LIST_ID");
				sql = "update " + PARTY_CONTACT_TABLE_NAME + " set ADDRESS_1 = ? where address_id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setString(1, partyContact.getAddress1());
				pstmt.setLong(2, partyContact.getAddressId());
				pstmt.executeUpdate();
				
				logger.info(">>> address exists, update sql={} ", sql);
				
			}
			resultSet.close();
			
			if (count == 0) {
				// insert 
				sql = "insert into " + PARTY_CONTACT_TABLE_NAME + " (ROLE_TYPE,LIST_ID,ADDRESS_ID,ADDRESS_1)" + " values (?,?,?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, partyContact.getRoleType());
				pstmt.setLong(2, partyContact.getAddressId());
				pstmt.setLong(3, partyContact.getAddressId());
				pstmt.setString(4, partyContact.getAddress1());
				pstmt.executeUpdate();
				
				logger.info(">>> no address exists, insert sql={} ", sql);
			}
			pstmt.close();
			conn.close();
			
		} else {
			sql = "select count(*) AS COUNT from " + PARTY_CONTACT_TABLE_NAME 
					+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
			int count = getCount(sql);
			if (count == 0) {
				Connection conn = connPool.getConnection();
				sql = "insert into " + PARTY_CONTACT_TABLE_NAME + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
						+ " values (?,?,?,?,?,?,?,?,?)";
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
				conn.close();
			} else {
				// record exists, error
				String error = String.format("table=%s record already exists, role_type=%d, list_id=%d", PARTY_CONTACT_TABLE_NAME, partyContact.getRoleType(), partyContact.getListId());
				throw new Exception(error);
			}
			
		}
	}
	private void doDelete(ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String tableName = payload.get("TABLE_NAME").asText();
		logger.info(">>> delete tableName={}", tableName);
		String before = payload.get("before").toString();
		Connection conn = connPool.getConnection();
		String sql = "";
		Integer roleType = null;
		Long listId = null;
		if (POLICY_HOLDER_TABLE_NAME.equals(tableName)) {
			PolicyHolder policyHolder = objectMapper.readValue(before, PolicyHolder.class);
			roleType = POLICY_HOLDER_ROLE_TYPE;
			listId = policyHolder.getListId();
		} else if (INSURED_LIST_TABLE_NAME.equals(tableName)) {
			InsuredList insuredList = objectMapper.readValue(before, InsuredList.class);
			roleType = INSURED_LIST_ROLE_TYPE;
			listId = insuredList.getListId();
		} else if (CONTRACT_BENE_TABLE_NAME.equals(tableName)) {
			ContractBene contractBene = objectMapper.readValue(before, ContractBene.class);
			roleType = CONTRACT_BENE_ROLE_TYPE;
			listId = contractBene.getListId();
		} else if (ADDRESS_TABLE_NAME.equals(tableName)) {
			Address address = objectMapper.readValue(before, Address.class);
			roleType = ADDRESS_ROLE_TYPE;
			listId = address.getAddressId();
		}
		logger.info(">>> roleType={},listId={}", roleType, listId);

		sql = "select count(*) AS COUNT from " + PARTY_CONTACT_TABLE_NAME + " where role_type = ? and list_id = ?";
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
			sql = "delete " + PARTY_CONTACT_TABLE_NAME + " where role_type = ? and list_id = ?"; 
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, roleType);
			pstmt.setLong(2, listId);

			pstmt.executeUpdate();
		} else {
			// record exists, error
			String error = String.format("table=%s record does not exist, role_type=%d, list_id=%d", PARTY_CONTACT_TABLE_NAME, roleType, listId);
			throw new Exception(error);
		}
		pstmt.close();

		conn.close();
	}
	private void doUpdate(ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String tableName = payload.get("TABLE_NAME").asText();
		logger.info(">>> update tableName={}", tableName);
		String data = payload.get("data").toString();
		String before = payload.get("before").toString();
		Connection conn = connPool.getConnection();
		String sql = "";
		PartyContact oldpartyContact = objectMapper.readValue(before, PartyContact.class);
		PartyContact newpartyContact = objectMapper.readValue(data, PartyContact.class);

		if (POLICY_HOLDER_TABLE_NAME.equals(tableName)) {
			oldpartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
			newpartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
		} else if (INSURED_LIST_TABLE_NAME.equals(tableName)) {
			oldpartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
			newpartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
		} else if (CONTRACT_BENE_TABLE_NAME.equals(tableName)) {
			oldpartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
			newpartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
		} else if (ADDRESS_TABLE_NAME.equals(tableName)) {
			oldpartyContact.setRoleType(ADDRESS_ROLE_TYPE);
			newpartyContact.setRoleType(ADDRESS_ROLE_TYPE);
			oldpartyContact.setListId(oldpartyContact.getAddressId());
			newpartyContact.setListId(newpartyContact.getAddressId());
		}
		logger.info(">>> oldpartyContact={}", oldpartyContact);
		logger.info(">>> newpartyContact={}", newpartyContact);

		if (ADDRESS_TABLE_NAME.equals(tableName)) {
			sql = "select count(*) AS COUNT from " + PARTY_CONTACT_TABLE_NAME 
					+ " where address_id = " + oldpartyContact.getAddressId();
		} else {
			sql = "select count(*) AS COUNT from " + PARTY_CONTACT_TABLE_NAME 
					+ " where role_type = " + oldpartyContact.getRoleType() + " and list_id = " + oldpartyContact.getListId();
		}
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet resultSet = pstmt.executeQuery();
		Integer count = 0; 
		while (resultSet.next()) {
			count = resultSet.getInt("COUNT");
		}
		logger.info(">>> count={}", count);
		resultSet.close();
		pstmt.close();

		if (count > 0) {
			if (ADDRESS_TABLE_NAME.equals(tableName)) {
				StringBuilder sb = new StringBuilder();
				if (!Objects.equals(oldpartyContact.getAddress1(), newpartyContact.getAddress1())) {
					sb.append(",ADDRESS_1='" + newpartyContact.getAddress1()+"'");
				}
				if (StringUtils.isNotBlank(sb.toString())) {
					sql = "update " + PARTY_CONTACT_TABLE_NAME 
							+ " set " + sb.toString().substring(1) 
							+ " where address_id = ?";
					logger.info(">>> update from address, sql={}", sql);
					
					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, newpartyContact.getListId());
					
					pstmt.executeUpdate();
					pstmt.close();
					
					
				}
			} else {
				StringBuilder sb = new StringBuilder();
				if (!Objects.equals(oldpartyContact.getPolicyId(), newpartyContact.getPolicyId())) {
					sb.append(",POLICY_ID=" + newpartyContact.getPolicyId());
				}
				if (!Objects.equals(oldpartyContact.getName(), newpartyContact.getName())) {
					sb.append(",NAME='" + newpartyContact.getName()+"'");
				}
				if (!Objects.equals(oldpartyContact.getCertiCode(), newpartyContact.getCertiCode())) {
					sb.append(",CERTI_CODE='" + newpartyContact.getCertiCode()+"'");
				}
				if (!Objects.equals(oldpartyContact.getMobileTel(), newpartyContact.getMobileTel())) {
					sb.append(",MOBILE_TEL='" + newpartyContact.getMobileTel()+"'");
				}
				if (!Objects.equals(oldpartyContact.getEmail(), newpartyContact.getEmail())) {
					sb.append(",EMAIL='" + newpartyContact.getEmail()+"'");
				}
				if (!Objects.equals(oldpartyContact.getAddressId(), newpartyContact.getAddressId())) {
					sb.append(",ADDRESS_ID=" + newpartyContact.getAddressId());
				}
				if (StringUtils.isNotBlank(sb.toString())) {
					pstmt.close();
					sql = "update " + PARTY_CONTACT_TABLE_NAME 
							+ " set " + sb.toString().substring(1) 
							+ " where role_type = ? and list_id = ?";
					logger.info(">>> update from party, sql={}", sql);
					
					pstmt = conn.prepareStatement(sql);
					pstmt.setInt(1, newpartyContact.getRoleType());
					pstmt.setLong(2, newpartyContact.getListId());
					
					pstmt.executeUpdate();
					
					pstmt.close();
				}
			}
		} else {
			// record exists, error
			String error = String.format("table=%s record does not exist, role_type=%d, list_id=%d", PARTY_CONTACT_TABLE_NAME, oldpartyContact.getRoleType(), oldpartyContact.getListId());
			throw new Exception(error);
		}

		conn.close();

	}
	private void createTable() throws Exception {

		Connection conn = connPool.getConnection();

		boolean createTable = false;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();

			stmt.executeQuery("select count(*) from " + PARTY_CONTACT_TABLE_NAME);

		} catch (java.sql.SQLException e) {
			logger.info(">>> err mesg={}, continue to create table", e.getMessage());

			// assume sink table does not exists
			// create table
			createTable = true;

		}
		stmt.close();

		if (createTable) {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();	
			try (InputStream inputStream = loader.getResourceAsStream("createtable-T_PARTY_CONTACT.sql")) {
				String createTableScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
				stmt = conn.createStatement();
				stmt.executeUpdate(createTableScript);
			} catch (SQLException | IOException e) {
				if (stmt != null) stmt.close();
				throw e;
			}
		}

		conn.close();

	}
	
	private Integer getCount(String sql) throws SQLException {
		Connection conn = connPool.getConnection();
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet resultSet = pstmt.executeQuery();
		Integer count = 0; 
		while (resultSet.next()) {
			count = resultSet.getInt("COUNT");
		}
		logger.info(">>> count={}", count);
		resultSet.close();
		pstmt.close();
		
		conn.close();
		
		return count;
	}

}
