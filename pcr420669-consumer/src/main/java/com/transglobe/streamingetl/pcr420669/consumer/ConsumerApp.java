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
						count++;
						logger.info("   >>>count={}", count);
						Connection conn = null;
						try {
							logger.info("   >>>record.value()={}", record.value());
							JsonNode jsonNode = objectMapper.readTree(record.value());
							String tableName = jsonNode.get("payload").get("TABLE_NAME").asText();//"T_POLICY_HOLDER"							
							String operation = jsonNode.get("payload").get("OPERATION").asText();
							
							logger.info("   >>>tableName={}, operation={}", tableName, operation);
							
							if ("INSERT".equals(operation)) {
								String data = jsonNode.get("payload").get("data").toString();
								ObjectMapper objectMapper2 = new ObjectMapper();
								PolicyHolder policyHolder = objectMapper2.readValue(data, PolicyHolder.class);
								logger.info("   >>>insert PolicyHolder={}", ToStringBuilder.reflectionToString(policyHolder));
								
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
								String sinkSqlRedo = jsonNode.get("payload").get("SINK_SQL_REDO").asText();
								logger.info("   >>>update,sink redoStr={}", sinkSqlRedo);
								//								
								//								pstmt2 = conn.prepareStatement(sinkSqlRedo);
								//								pstmt2.executeBatch();

							} else if ("DELETE".equals(operation)) {
								String before = jsonNode.get("payload").get("before").toString();
								ObjectMapper objectMapper2 = new ObjectMapper();
								PolicyHolder policyHolder = objectMapper2.readValue(before, PolicyHolder.class);
								logger.info("   >>> delete PolicyHolder={}", ToStringBuilder.reflectionToString(policyHolder));
								
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
