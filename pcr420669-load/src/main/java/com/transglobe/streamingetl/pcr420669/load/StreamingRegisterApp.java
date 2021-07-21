package com.transglobe.streamingetl.pcr420669.load;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingRegisterApp {
	private static final Logger logger = LoggerFactory.getLogger(StreamingRegisterApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	private static final String LOGMINER_TABLE_STREAMING_ETL_HEALTH_TABLE_FILE_NAME = "logminertable-T_STREAMING_ETL_HEALTH_CDC.sql";
	private static final String LOGMINER_TABLE_STREAMING_REGISTER_FILE_NAME = "logminertable-T_STREAMING_REGISTER.sql";
	
	private Config config;

	public StreamingRegisterApp(String configFile) throws Exception {
		config = Config.getConfig(configFile);
	}
	private void init() throws Exception {
		String tableName = config.logminerTableStreamingEtlHealthCdc;
		logger.info(">>> check table exists:{}", tableName);
		if (!checkTableExists(tableName)) {
			logger.info(">>> table:{}, does not exists", tableName);
			// create table
			String tableFileName = LOGMINER_TABLE_STREAMING_ETL_HEALTH_TABLE_FILE_NAME;
			logger.info(">>> create table:{}", tableFileName);
			createTable(tableFileName);
		} else {
			logger.info(">>> table:{}, exists", tableName);
		}

		tableName = config.logminerTableStreamingRegister;
		logger.info(">>> check table exists:{}", tableName);
		if (!checkTableExists(tableName)) {
			logger.info(">>> table:{}, does not exists", tableName);
			// create table
			String tableFileName = LOGMINER_TABLE_STREAMING_REGISTER_FILE_NAME;
			logger.info(">>> create table:{}", tableFileName);
			createTable(tableFileName);
		} else {
			logger.info(">>> table:{}, exists", tableName);
		}
	}
	private void createTable(String createTableFile) throws Exception {
		Connection sourceConn = null;
		Statement stmt = null;
		InputStream inputStream = null;
		String sql = null;
		try {
			Class.forName(config.logminerDbDriver);

			sourceConn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);

			ClassLoader loader = Thread.currentThread().getContextClassLoader();	
			inputStream = loader.getResourceAsStream(createTableFile);
			sql = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			
			stmt = sourceConn.createStatement();
			
			stmt.execute(sql);
		} catch (Exception e) {
			
			throw e;
		} finally {
			if (inputStream != null) inputStream.close();
			if (stmt != null) stmt.close();
			if (sourceConn != null) sourceConn.close();
		}


	}
	private boolean checkTableExists(String tableName) throws Exception {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		Console console = null;
		boolean exists = false;
		try {
			Class.forName(config.logminerDbDriver);

			sourceConn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);

			sql = "select count(*) from " + tableName;

			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				exists = true;
			}
			rs.close();
			pstmt.close();


			//			console = System.console();
			//			console.printf(" time:%d, currentScn:%d", time, currentScn);
			//			console.flush();

		} catch (Exception e) {
			if (e instanceof SQLException) {
				logger.error(">>> sqlstate:{}", ((SQLException) e).getSQLState());
				logger.error(">>> errorCode:{}", ((SQLException) e).getErrorCode());
				int errorCode = ((SQLException) e).getErrorCode();
				if (942 == errorCode) {
					// table does not exists
				}
			} else {
				throw e;
			}
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					throw e;
				}
			}

		}
		return exists;
	}
	private void run() throws Exception {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		Console console = null;

		try {
			Class.forName(config.logminerDbDriver);

			sourceConn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);

			sourceConn.setAutoCommit(false);

			sql = "select CURRENT_SCN from v$database";
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			long currentScn = 0L;
			while (rs.next()) {
				currentScn = rs.getLong("CURRENT_SCN");
			}
			rs.close();
			pstmt.close();


			long time = System.currentTimeMillis();
			sql = "insert into " + config.logminerTableStreamingRegister
					+ " (time,current_scn) " 
					+ " values (?,?)";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, time);
			pstmt.setLong(1, currentScn);
			pstmt.executeUpdate();
			sourceConn.commit();

			pstmt.close();

			console = System.console();
			console.printf(" time:%d, currentScn:%d", time, currentScn);
			console.flush();

		} catch (Exception e) {
			throw e;
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					throw e;
				}
			}

		}

	}

	public static void main(String[] args) {
		String profileActive = System.getProperty("profile.active", "");

		StreamingRegisterApp app;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new StreamingRegisterApp(configFile);

			app.init();

			//			app.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
