package com.transglobe.streamingetl.pcr420669.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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


public class ConsumerApp {

	static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	private Config config;

	public ConsumerApp(String fileName) throws Exception {
		config = Config.getConfig(fileName);
	}
	public static void main(String[] args) {
		ConsumerApp app = null;
		try {
			app = new ConsumerApp(CONFIG_FILE_NAME);

			app.createTopics();
			
			app.run();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
	private void run() {
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

						try {
							logger.info("   >>>record.value()={}", record.value());
							JsonNode jsonNode = objectMapper.readTree(record.value());
							String operation = jsonNode.get("payload").get("OPERATION").asText();
							if ("INSERT".equals(operation)) {
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

							}

						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							logger.error(e.getMessage(), e);

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
			logger.error("Consumer error", e);
			System.exit(1);
		} catch (Throwable e) {

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
		System.out.println("-- listing --");
		admin.listTopics().names().get().forEach(System.out::println);
				
		//creating new topic
		System.out.println("-- creating --");
		for (String topic : topicList) {
			if (!existingTopics.contains(topic)) {
				NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
				admin.createTopics(Collections.singleton(newTopic));
			}
		}

		
	}
}
