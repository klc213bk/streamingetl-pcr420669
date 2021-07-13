package com.transglobe.streamingetl.pcr420669.load;

import java.io.Console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class HealthCheckApp {
	private static final String CONFIG_FILE_NAME = "config.properties";

	private Config config;

	public HealthCheckApp(String configFile) throws Exception {
		config = Config.getConfig(configFile);
	}

	private void run() throws Exception {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		String sql = "";
		Console console = null;
		while (true) {
			try {
				Class.forName(config.healthDbDriver);
				
				sourceConn = DriverManager.getConnection(config.healthDbUrl, config.healthDbUsername, config.healthDbPassword);

				sourceConn.setAutoCommit(false);

				long time = System.currentTimeMillis();
				sql = "insert into " + config.healthTableStreamingEtlHealthCdc
						+ " (cdc_time) " 
						+ " values (?)";
				pstmt = sourceConn.prepareStatement(sql);
				pstmt.setLong(1, time);
				pstmt.executeUpdate();
				sourceConn.commit();
				
				pstmt.close();
				
				console = System.console();
				console.printf(" %d ", time);
				console.flush();

				
				Thread.sleep(60000);
				

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
	}

	public static void main(String[] args) {
		String profileActive = System.getProperty("profile.active", "");

		HealthCheckApp app;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new HealthCheckApp(configFile);

			app.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
