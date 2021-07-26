package com.transglobe.streamingetl.pcr420669.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestApp2 {
	private static final Logger logger = LoggerFactory.getLogger(TestApp2.class);
	
	static String dbDriver = "org.apache.ignite.IgniteJdbcThinDriver";
	static String sinkDbUrl = "jdbc:ignite:thin://127.0.0.1:10850;user=ignite;password=ignite";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			test1();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void test1() throws Exception {
		
		
		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(dbDriver);
			//	logger.info(">>driver={}, sinkDbUrl={},sinkDbUsername={},sinkDbPassword={}", config.sinkDbDriver, config.sinkDbUrl);
			sinkConn = DriverManager.getConnection(sinkDbUrl, null, null);
			
			sql = "select role_type,list_id,policy_id,name,certi_code,mobile_tel,email,address_id,address_1 "
					+ " from T_PARTY_CONTACT "
					+ " where list_id = ?";
			
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, 19830034);
			
			rs = pstmt.executeQuery();
			
			Map<String, Object> map = new HashMap<>();
			while (rs.next()) {
				map.put("ROLE_TYPE", (rs.getInt("ROLE_TYPE") == 0)? null : rs.getInt("ROLE_TYPE"));
				map.put("LIST_ID", (rs.getLong("LIST_ID") == 0)? null : rs.getLong("LIST_ID"));
				map.put("POLICY_ID", (rs.getLong("POLICY_ID") == 0)? null : rs.getLong("POLICY_ID"));
				map.put("NAME", rs.getString("NAME"));
				map.put("CERTI_CODE", rs.getString("CERTI_CODE"));
				map.put("MOBILE_TEL", rs.getString("MOBILE_TEL"));
				map.put("EMAIL", rs.getString("EMAIL"));
				map.put("ADDRESS_ID", (rs.getLong("ADDRESS_ID") == 0)? null : rs.getLong("ADDRESS_ID"));
				map.put("ADDRESS_1", rs.getString("ADDRESS_1"));
				
				logger.info(">>> map={}", map);
			}
			
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
			if (sinkConn != null) sinkConn.close();
		}
	}

}
