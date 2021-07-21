package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInsertDataApp {
	static final Logger logger = LoggerFactory.getLogger(TestInsertDataApp.class);
	
	private static String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static String DB_URL = "jdbc:oracle:thin:@10.67.67.63:1521:ebaouat1";
	private static String DB_USERNAME = "ls_ebao";
	private static String DB_PASSWORD = "ls_ebaopwd";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			test();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void test() throws Exception {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			Class.forName(DB_DRIVER);
			//	logger.info(">>driver={}, sourceDbUrl={},sourceDbUsername={},sourceDbPassword={}", config.sourceDbDriver, config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);
			sourceConn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
			
			sourceConn.setAutoCommit(false);
			
			String sql = "select * from test_t_policy_holder1";
			
			pstmt = sourceConn.prepareStatement(sql);
			
			rs = pstmt.executeQuery();
			
			List<Long> listIdList = new ArrayList<>();
			while (rs.next()) {
				logger.info("list id={}", rs.getLong("LIST_ID"));
				listIdList.add(rs.getLong("LIST_ID"));
			}
			rs.close();
			pstmt.close();
			
			sql = "insert into test_t_policy_holder "
					+ " (select * from test_t_policy_holder1"
					+ " where list_id = ?)";
			pstmt = sourceConn.prepareStatement(sql);
			int size = 1; //listIdList.size();
			for (int i = 0; i < size; i++) {
				logger.info("sql={}, id={}", sql, listIdList.get(i));
				pstmt.setLong(1, listIdList.get(i));
				pstmt.executeUpdate();
				sourceConn.commit();
				Thread.sleep(10000);
			}
			
			pstmt.close();

		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
		}

	}

}
