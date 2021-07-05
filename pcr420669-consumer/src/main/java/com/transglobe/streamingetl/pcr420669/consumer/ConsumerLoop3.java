package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
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

/**
 * 
 * Decoupling polling and processing
 * @author steven
 *
 */
public class ConsumerLoop3 implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(ConsumerLoop3.class);

	private final KafkaConsumer<String, String> consumer;
	
	private Config config;

	private BasicDataSource sourceConnPool;
	private BasicDataSource sinkConnPool;

	private ExecutorService executorService;
	
	public ConsumerLoop3(int id,
			String groupId,  
			Config config,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool) {

		this.config = config;
		this.sourceConnPool = sourceConnPool;
		this.sinkConnPool = sinkConnPool;
		Properties props = new Properties();
		props.put("bootstrap.servers", config.bootstrapServers);
		props.put("group.id", groupId);
		props.put("client.id", groupId + "-" + id );
		props.put("group.instance.id", groupId + "-mygid" );
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);
		
		executorService = Executors.newFixedThreadPool(10);
	}

	@Override
	public void run() {
		try {
			consumer.subscribe(config.topicList);

			logger.info("   >>>>>>>>>>>>>>>>>>>>>>>> run ..........");
			
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

				if (records.count() > 0) {
					//Connection sinkConn = null;
				//Connection sourceConn = null;
					int tries = 0;

					while (sourceConnPool.isClosed()) {
						tries++;
						try {
							sourceConnPool.restart();

							logger.info("   >>> Source Connection Pool restart, try {} times", tries);

							Thread.sleep(30000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}
					}
					tries = 0;
					while (sinkConnPool.isClosed()) {
						tries++;
						try {
							sinkConnPool.restart();

							logger.info("   >>> Sink Connection Pool restart, try {} times", tries);

							Thread.sleep(30000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}
					}
					
					for (ConsumerRecord<String, String> record : records) {
						ProcessJob job = new ProcessJob(record, sourceConnPool, sinkConnPool, config);
						executorService.submit(job);
					}
	
				}
			}
		} catch (Exception e) {
			// ignore for shutdown 
			logger.error(">>>Consumer error, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			consumer.close();

			if (sourceConnPool != null) {
				try {
					sourceConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>sourceConnPool close error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
			if (sinkConnPool != null) {
				try {
					sinkConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>sinkConnPool error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
		}
	}
	
	public void shutdown() {
		
		consumer.wakeup();
	}
}