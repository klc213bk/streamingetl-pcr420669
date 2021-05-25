package com.transglobe.streamingetl.pcr420669.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerApp {
	static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	
	private static final String GROUP_ID = "consumer-pcr420669-group";
	
	private static final int NUM_CONSUMERS = 1;

	public static void main(String[] args) { 
		String profileActive = System.getProperty("profile.active", "");
		String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

		Config config = null;
		try {
			config = Config.getConfig(configFile);
		} catch (Exception e1) {
			logger.error(">>>message={}, stack trace={}", e1.getMessage(), ExceptionUtils.getStackTrace(e1));
			e1.printStackTrace();
		}

		String groupId = GROUP_ID;
		List<String> topics = Arrays.asList(
				"ebao.cdc.test_t_policy_holder.0",
				"ebao.cdc.test_t_insured_list.0",
				"ebao.cdc.test_t_contract_bene.0",
				"ebao.cdc.test_t_address.0",
				"ebao.cdc.t_streaming_etl_health_cdc.0"
				);
		ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);

		final List<ConsumerLoop> consumers = new ArrayList<>();
		for (int i = 0; i < NUM_CONSUMERS; i++) {
			ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics, config);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerLoop consumer : consumers) {
					consumer.shutdown();
				} 
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
					logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
					
				}
			}
		});
	}

}
