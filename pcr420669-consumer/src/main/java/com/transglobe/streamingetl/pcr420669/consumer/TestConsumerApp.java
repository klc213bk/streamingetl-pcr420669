package com.transglobe.streamingetl.pcr420669.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConsumerApp {
	static final Logger logger = LoggerFactory.getLogger(TestConsumerApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	
	private static final int NUM_CONSUMERS = 1;

	private static BasicDataSource sinkConnPool;
	private static BasicDataSource sourceConnPool;
	
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

		String groupId = config.groupId;
		
		sourceConnPool = new BasicDataSource();
		sourceConnPool.setUrl(config.sourceDbUrl);
		sourceConnPool.setUsername(config.sourceDbUsername);
		sourceConnPool.setPassword(config.sourceDbPassword);
		sourceConnPool.setDriverClassName(config.sourceDbDriver);
		sourceConnPool.setMaxTotal(3);
		
		sinkConnPool = new BasicDataSource();
		sinkConnPool.setUrl(config.sinkDbUrl);
		sinkConnPool.setUsername(null);
		sinkConnPool.setPassword(null);
		sinkConnPool.setDriverClassName(config.sinkDbDriver);
		sinkConnPool.setMaxTotal(3);
		
		ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);

		final List<ConsumerLoop2> consumers = new ArrayList<>();
		for (int i = 0; i < NUM_CONSUMERS; i++) {
			ConsumerLoop2 consumer = new ConsumerLoop2((i+1), groupId, config, sourceConnPool, sinkConnPool);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerLoop2 consumer : consumers) {
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
