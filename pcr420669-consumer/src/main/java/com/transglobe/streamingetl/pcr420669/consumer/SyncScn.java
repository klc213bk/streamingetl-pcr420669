package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SyncScn implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(SyncScn.class);
	private Config config;

	Connection conn = null;
	Connection sinkConn = null;
	PreparedStatement pstmt = null;
	ResultSet rs = null;
	
	public SyncScn(Config config) {
		this.config = config;
	}

	@Override
	public void run() {
		logger.info(">>> SyncScn is running ....");
		try {
			while (true) {
				String sql;

				try {
					Class.forName(config.sinkDbDriver);
					sinkConn = DriverManager.getConnection(config.sinkDbUrl);
					
					Class.forName(config.logminerDbDriver);
					conn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);
					conn.setAutoCommit(false);
					
					sql = "select SCN from " + config.sinkTableSupplLogSync 
							+ " order by INSERT_TIME desc limit 1";
					pstmt = sinkConn.prepareStatement(sql);
					rs = pstmt.executeQuery();
					long scn = 0L; 
					while (rs.next()) {
						scn = rs.getLong("SCN");
					}
					rs.close();
					pstmt.close();
					
					if (scn == 0L) {
						throw new Exception ("select no value from " + config.sinkTableSupplLogSync +", no update scn");
					}
					long now = System.currentTimeMillis();
					
					logger.info(">>> now={}", now);

					sql = "update " + config.logminerTableLogminerScn 
							+ " set SCN=?, SCN_UPDATE_TIME=? where STREAMING_NAME=?";

					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, scn);
					pstmt.setTimestamp(2, new Timestamp(now));
					pstmt.setString(3, config.streamingName);
					
					pstmt.executeUpdate();
					conn.commit();
					
					pstmt.close();

					logger.info(">>> update {}, scn={}, now={}", config.sinkTableSupplLogSync, scn, now);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				} finally {
					try {
						if (rs != null) rs.close();
						if (pstmt != null) pstmt.close();
						if (conn != null) conn.close();
						if (sinkConn != null) sinkConn.close();
					} catch (Exception e) {
						logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
					}
				}
				logger.info(">>> SyncScn sleep ");
				Thread.sleep(2 * config.syncscnPeriodMinute * 60 * 1000);
				logger.info(">>> SyncScn wake up from sleep");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		logger.info(">>> SyncScn Stop!!!");

	}
	public void shutdown() {
		logger.info(">>> SyncScn shutdown!!!");
		try {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
			if (sinkConn != null) sinkConn.close();
		} catch (Exception e) {
			logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
}
