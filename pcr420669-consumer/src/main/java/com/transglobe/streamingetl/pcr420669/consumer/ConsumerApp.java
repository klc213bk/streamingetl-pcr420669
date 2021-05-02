package com.transglobe.streamingetl.pcr420669.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
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
import com.transglobe.streamingetl.pcr420669.consumer.model.InsuredList;
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
		config = Config.getConfig(fileName);

		connPool = new BasicDataSource();
		connPool.setUrl(config.sinkDbUrl);
		connPool.setUsername(config.sinkDbUsername);
		connPool.setPassword(config.sinkDbPassword);
		connPool.setDriverClassName(config.sinkDbDriver);
		connPool.setMaxTotal(2);
	}
	public static void main(String[] args) {
		ConsumerApp app = null;
		try {
			app = new ConsumerApp(CONFIG_FILE_NAME);

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
								//								String updateSql = getUpdateSql(jsonNode, sinkFullTableName);
								String data = payload.get("data").toString();
								String before = payload.get("before").toString();
								PolicyHolder dataPolicyHolder = objectMapper.readValue(data, PolicyHolder.class);
								PolicyHolder beforePolicyHolder = objectMapper.readValue(before, PolicyHolder.class);
								logger.info("   >>>update dataPolicyHolder, listid={},policyid={},name={},certicode={},mobiletel={},email={},addressid={}",
										dataPolicyHolder.getListId()
										, dataPolicyHolder.getPolicyId()
										, dataPolicyHolder.getName()
										, dataPolicyHolder.getCertiCode()
										, dataPolicyHolder.getMobileTel()
										, dataPolicyHolder.getEmail()
										, dataPolicyHolder.getAddressId());
								logger.info("   >>>update beforePolicyHolder, listid={},policyid={},name={},certicode={},mobiletel={},email={},addressid={}",
										beforePolicyHolder.getListId()
										, beforePolicyHolder.getPolicyId()
										, beforePolicyHolder.getName()
										, beforePolicyHolder.getCertiCode()
										, beforePolicyHolder.getMobileTel()
										, beforePolicyHolder.getEmail()
										, beforePolicyHolder.getAddressId());

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
		Connection conn = connPool.getConnection();
		String sql = "";
		if (POLICY_HOLDER_TABLE_NAME.equals(tableName)) {
			PolicyHolder policyHolder = objectMapper.readValue(data, PolicyHolder.class);
			sql = "select count(*) AS COUNT from " + PARTY_CONTACT_TABLE_NAME + " where role_type = ? and list_id = ?";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, POLICY_HOLDER_ROLE_TYPE);
			pstmt.setLong(2, policyHolder.getListId());
			ResultSet resultSet = pstmt.executeQuery();
			Integer count = 0; 
			while (resultSet.next()) {
				count = resultSet.getInt("COUNT");
			}
			resultSet.close();
			pstmt.close();

			if (count == 0) {
				// insert into party_contact
				sql = "insert into " + PARTY_CONTACT_TABLE_NAME + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID) " 
								+ " values (?,?,?,?,?,?,?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, POLICY_HOLDER_ROLE_TYPE);
				pstmt.setLong(2, policyHolder.getListId());
				pstmt.setLong(3, policyHolder.getPolicyId());
				pstmt.setString(4, policyHolder.getName());
				pstmt.setString(5, policyHolder.getCertiCode());
				pstmt.setString(6, policyHolder.getMobileTel());
				pstmt.setString(7, policyHolder.getEmail());
				pstmt.setLong(8, policyHolder.getAddressId());

				pstmt.executeUpdate();

				pstmt.close();
			} else {
				// record exists, error
				String error = String.format("table=%s record already exists, role_type=%d, list_id=%d", PARTY_CONTACT_TABLE_NAME, POLICY_HOLDER_ROLE_TYPE, policyHolder.getListId());
				throw new Exception(error);
			}

		} else if (INSURED_LIST_TABLE_NAME.equals(tableName)) {

		} else if (CONTRACT_BENE_TABLE_NAME.equals(tableName)) {

		} else if (ADDRESS_TABLE_NAME.equals(tableName)) {
			Address address = objectMapper.readValue(data, Address.class);
			sql = "select count(*) AS COUNT from " + PARTY_CONTACT_TABLE_NAME + " where address_id = ?";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());
			ResultSet resultSet = pstmt.executeQuery();
			Integer count = 0; 
			while (resultSet.next()) {
				count = resultSet.getInt("COUNT");
			}
			resultSet.close();
			pstmt.close();

			if (count == 0) {
				// insert into party_contact
				sql = "insert into " + PARTY_CONTACT_TABLE_NAME + " (ROLE_TYPE,LIST_ID,ADDRESS_ID) " 
								+ " values (?,?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, 0);
				pstmt.setLong(2, address.getAddressId());
				pstmt.setLong(3, address.getAddressId());

				pstmt.executeUpdate();

				pstmt.close();
			} else {
				// record exists, error
				String error = String.format("table=%s record already exists, address_id=%d", PARTY_CONTACT_TABLE_NAME, POLICY_HOLDER_ROLE_TYPE, address.getAddressId());
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
			roleType = POLICY_HOLDER_ROLE_TYPE;
			listId = insuredList.getListId();
		}  else if (ADDRESS_TABLE_NAME.equals(tableName)) {
			Address address = objectMapper.readValue(before, Address.class);
			roleType = ADDRESS_ROLE_TYPE;
			listId = address.getAddressId();
		}
		logger.info(">>> droleType={},listId={}", roleType, listId);
		
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

}
