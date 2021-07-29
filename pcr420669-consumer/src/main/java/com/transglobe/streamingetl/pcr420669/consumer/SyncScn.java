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
				boolean updateScn = true;
				try {
					Class.forName(config.sinkDbDriver);
					sinkConn = DriverManager.getConnection(config.sinkDbUrl);
					
					Class.forName(config.logminerDbDriver);
					conn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);
					conn.setAutoCommit(false);
					
					sql = "select SCN,INSERT_TIME from " + config.sinkTableSupplLogSync 
							+ " order by INSERT_TIME desc limit 1";
					pstmt = sinkConn.prepareStatement(sql);
					rs = pstmt.executeQuery();
					long scn = 0L; 
					long insertTime = 0L;
					while (rs.next()) {
						scn = rs.getLong("SCN");
						insertTime = rs.getLong("INSERT_TIME");
					}
					rs.close();
					pstmt.close();
					
					if (scn == 0L) {
						updateScn = false;
						throw new Exception ("select no value from " + config.sinkTableSupplLogSync +", no update scn");
					}

					sql = "update " + config.logminerTableLogminerScn 
							+ " set SCN=?, SCN_UPDATE_TIME=? where STREAMING_NAME=?";

					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, scn);
					pstmt.setTimestamp(2, new Timestamp(insertTime));
					pstmt.setString(3, config.streamingName);
					
					pstmt.executeUpdate();
					conn.commit();
					
					pstmt.close();

					logger.info(">>> update {}, scn={}, insertTime={}, now={}", config.sinkTableSupplLogSync, scn, insertTime, System.currentTimeMillis());
				} catch (Exception e) {
					if (!updateScn) {
						logger.info(">>> message={}", e.getMessage());
					} else {
						logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
					}
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
				Thread.sleep(config.syncscnPeriodMinute * 60 * 1000);
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
