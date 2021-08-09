package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Cleanup implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(Cleanup.class);
	private Config config;

	Connection conn = null;
	PreparedStatement pstmt = null;
	ResultSet rs = null;

	public Cleanup(Config config) {
		this.config = config;
	}

	@Override
	public void run() {
		logger.info(">>> Cleanup is running ....");
		try {
			while (true) {
				String sql;

				try {
					Class.forName(config.sinkDbDriver);
					conn = DriverManager.getConnection(config.sinkDbUrl);
					conn.setAutoCommit(false);

					long now = System.currentTimeMillis();
					long t = now  - config.cleanupPeriodMinute * 60 * 1000; //

					sql = "select count(*) as CNT from " + config.sinkTableSupplLogSync + " where INSERT_TIME >= ?";
					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, t);
					rs = pstmt.executeQuery();
					int cnt = 0;
					while (rs.next()) {
						rs.getInt("CNT");
					}
					rs.close();
					pstmt.close();

					if (cnt > 100) {

						sql = "delete from " + config.sinkTableSupplLogSync + " where INSERT_TIME < ?";

						pstmt = conn.prepareStatement(sql);
						pstmt.setLong(1, t);

						pstmt.executeUpdate();
						conn.commit();
						pstmt.close();
					}

					logger.info(">>> sinkTableSupplLogSync deleted, now={}, where INSERT_TIME < {}", now, t);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				} finally {
					try {
						if (pstmt != null) pstmt.close();
						if (conn != null) conn.close();
					} catch (Exception e) {
						logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
					}
				}

				logger.info(">>> Clean up sleep ");
				Thread.sleep(config.cleanupPeriodMinute * 60 * 1000);
				logger.info(">>> Clean up wake up from sleep");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		logger.info(">>> Clean up Stop!!!");

	}
	public void shutdown() {
		logger.info(">>> Cleanup shutdown!!!");
		try {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		} catch (Exception e) {
			logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
}
