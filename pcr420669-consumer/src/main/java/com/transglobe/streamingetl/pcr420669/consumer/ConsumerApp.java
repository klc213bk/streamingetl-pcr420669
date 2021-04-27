package com.transglobe.streamingetl.pcr420669.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(config.topicList);

		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			if (!records.isEmpty()) {
				
				
				consumer.commitSync();
			}
		}
	}
}

}
