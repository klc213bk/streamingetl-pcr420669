package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.consumer.model.Address;
import com.transglobe.streamingetl.pcr420669.consumer.model.PartyContact;
import com.transglobe.streamingetl.pcr420669.consumer.model.StreamingEtlHealthCdc;

public class TestConsumerLoop implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(TestConsumerLoop.class);

	private static final Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static final Integer INSURED_LIST_ROLE_TYPE = 2;
	private static final Integer CONTRACT_BENE_ROLE_TYPE = 3;

	private final KafkaConsumer<String, String> consumer;
	private final int id;

	private Config config;

	private BasicDataSource sourceConnPool;
	private BasicDataSource sinkConnPool;

	public TestConsumerLoop(int id,
			String groupId,  
			Config config,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool) {
		this.id = id;
		this.config = config;
		this.sourceConnPool = sourceConnPool;
//		this.sinkConnPool = sinkConnPool;
		Properties props = new Properties();
		props.put("bootstrap.servers", config.bootstrapServers);
		props.put("group.id", groupId);
		props.put("client.id", groupId + "-" + id );
		props.put("group.instance.id", groupId + "-mygid" );
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
//		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000);
		this.consumer = new KafkaConsumer<>(props);

	}

	@Override
	public void run() {

		try {
			consumer.subscribe(config.topicList);

			logger.info("   >>>>>>>>>>>>>>>>>>>>>>>> run ..........");

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

				if (records.count() > 0) {
//					Connection sinkConn = null;
//					Connection sourceConn = null;
//					int tries = 0;
//
//					while (sinkConnPool.isClosed()) {
//						tries++;
//						try {
//							sinkConnPool.restart();
//
//							logger.info("   >>> Connection Pool restart, try {} times", tries);
//
//							Thread.sleep(30000);
//						} catch (Exception e) {
//							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
//						}
//
//					}
//					sinkConn = sinkConnPool.getConnection();

					for (ConsumerRecord<String, String> record : records) {
						Map<String, Object> data = new HashMap<>();
						Connection sourceConn = null;
						Connection sinkConn = null;
						try {	
//							sinkConn = sinkConnPool.getConnection();
//							sinkConn.setAutoCommit(false);
							data.put("partition", record.partition());
							data.put("offset", record.offset());
							data.put("value", record.value());
							//					System.out.println(this.id + ": " + data);

							ObjectMapper objectMapper = new ObjectMapper();
							objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

							JsonNode jsonNode = objectMapper.readTree(record.value());
							JsonNode payload = jsonNode.get("payload");
							//	payloadStr = payload.toString();

							String operation = payload.get("OPERATION").asText();

							String tableName = payload.get("TABLE_NAME").asText();
							logger.info("   >>>offset={},operation={}, TableName={}", record.offset(), operation, tableName);
							logger.info("   >>>data={}", data);
							
//							Thread.sleep(31000L);
							
							

						//	sinkConn.commit();
						} catch(Exception e) {
							logger.error(">>>message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e), data);
						} finally {
							if (sinkConn != null) sinkConn.close();
							if (sourceConn != null) sourceConn.close();
						}
					}
				}


			}
		} catch (Exception e) {
			// ignore for shutdown 
			logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			consumer.close();

//			if (sinkConnPool != null) {
//				try {
//					sinkConnPool.close();
//				} catch (SQLException e) {
//					logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
//				}
//			}
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}


	
	

}