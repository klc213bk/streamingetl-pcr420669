package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.consumer.model.PolicyHolder;


public class ConsumerApp {

	static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

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
						JsonNode jsonNode = objectMapper.readTree(record.value());
						JsonNode payload = jsonNode.get("payload");
						count++;
						logger.info("   >>>count={}", count);
						Connection conn = null;
						try {
							logger.info("   >>>record.value()={}", record.value());
							String tableName = payload.get("TABLE_NAME").asText();//"T_POLICY_HOLDER"							
							String operation = payload.get("OPERATION").asText();
							
							logger.info("   >>>tableName={}, operation={}", tableName, operation);
							
							if ("INSERT".equals(operation)) {
								String data = payload.get("data").toString();
								objectMapper = new ObjectMapper();
								PolicyHolder policyHolder = objectMapper.readValue(data, PolicyHolder.class);
								logger.info("   >>>insert PolicyHolder={}", ToStringBuilder.reflectionToString(policyHolder));
								logger.info("   >>>insert PolicyHolder, listid={},policyid={},name={},certicode={},mobiletel={},email={},addressid={}",
										policyHolder.getListId()
										, policyHolder.getPolicyId()
										, policyHolder.getName()
										, policyHolder.getCertiCode()
										, policyHolder.getMobileTel()
										, policyHolder.getEmail()
										, policyHolder.getAddressId());
										
								conn = connPool.getConnection();
								//								
								//								setPreparedStatement(jsonNode, pstmt);
								//							
								//								pstmt.addBatch();
								//								
								//								if (count % 100 == 0 || count == records.count()) {
								//									pstmt.executeBatch();//executing the batch  
								//									
								//									logger.info("   >>>count={}, execute batch", count);
								//								}
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
								String before = payload.get("before").toString();
								PolicyHolder beforePolicyHolder = objectMapper.readValue(before, PolicyHolder.class);
								logger.info("   >>>delete beforePolicyHolder, listid={},policyid={},name={},certicode={},mobiletel={},email={},addressid={}",
										beforePolicyHolder.getListId()
										, beforePolicyHolder.getPolicyId()
										, beforePolicyHolder.getName()
										, beforePolicyHolder.getCertiCode()
										, beforePolicyHolder.getMobileTel()
										, beforePolicyHolder.getEmail()
										, beforePolicyHolder.getAddressId());
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
}
