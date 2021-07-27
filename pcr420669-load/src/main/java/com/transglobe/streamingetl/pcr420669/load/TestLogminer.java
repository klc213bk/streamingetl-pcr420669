package com.transglobe.streamingetl.pcr420669.load;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InitialLoadApp [loaddata]
 *  dataload rule
 *  1. 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
 *  so, set email to null if role_type = 3
 *  
 *  
 *  select  a.LIST_ID,a.POLICY_ID,a.NAME, 
a.CERTI_CODE,a.MOBILE_TEL, a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1
  from %tlogtable% a 
  left join T_ADDRESS b on a.ADDRESS_ID = b.ADDRESS_ID 
  where LAST_CMT_FLG = 'Y'
union  
select a.LIST_ID,a.POLICY_ID,a.NAME, a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1
  from %ttable% A
 inner join T_CONTRACT_MASTER B ON A.POLICY_ID=B.POLICY_ID 
  left join T_ADDRESS c on A.ADDRESS_ID = C.ADDRESS_ID 
  where B.LIABILITY_STATE = 0;


where %ttable% = T_POLICY_HOLDER,T_INSURED_LIST,T_CONTRACT_BENE 
and %tlogtable% = T_POLICY_HOLDER_LOG,T_INSURED_LIST_LOG,T_CONTRACT_BENE_LOG

 * @author oracle
 *
 */
public class TestLogminer {
	private static final Logger logger = LoggerFactory.getLogger(TestLogminer.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	private static final int THREADS = 15;

	//	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;

	static class LoadBean {
		String tableName;
		Integer roleType;
		Long startSeq;
		Long endSeq;
	}
	private Config config;

	public TestLogminer(String fileName) throws Exception {
		config = Config.getConfig(fileName);
		//	this.createTableFile = createTableFile;

		sourceConnectionPool = new BasicDataSource();

		sourceConnectionPool.setUrl(config.sourceDbUrl);
		sourceConnectionPool.setUsername(config.sourceDbUsername);
		sourceConnectionPool.setPassword(config.sourceDbPassword);
		sourceConnectionPool.setDriverClassName(config.sourceDbDriver);
		sourceConnectionPool.setMaxTotal(THREADS);

		sinkConnectionPool = new BasicDataSource();
		sinkConnectionPool.setUrl(config.sinkDbUrl);
		sinkConnectionPool.setDriverClassName(config.sinkDbDriver);
		sinkConnectionPool.setMaxTotal(THREADS);

	}
	private void close() {
		try {
			if (sourceConnectionPool != null) sourceConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		try {
			if (sinkConnectionPool != null) sinkConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
	public static void main(String[] args) {
		logger.info(">>> start run InitialLoadApp");

		boolean noload = false;
		if (args.length != 0 && StringUtils.equals("noload", args[0])) {
			noload = true;
		}


		Long t0 = System.currentTimeMillis();

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;
			
			TestLogminer app = new TestLogminer(configFile);

			app.run();


			app.close();


			System.exit(0);

		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			System.exit(1);
		}

	}

	private void run() throws Exception {

		Long v_commit_scn = 7944073551728L;
		Long o_current_scn = 7944073556728L;
		Connection sourceConn = null;
		String sql = null;
		//PreparedStatement pstmt = null;
		Statement stmt = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		try {

			sourceConn = this.sourceConnectionPool.getConnection();
			
			Long v_scn = null;
			Long v_min_first_change;
			
			sql = "select current_scn from v$database";
			
			stmt = sourceConn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				v_scn = rs.getLong("CURRENT_SCN");
				logger.info("current_scn={} ", v_scn);
			}
			rs.close();
			stmt.close();	
			
			sql = "select min(first_change#) as v_min_first_change from v$log";
			stmt = sourceConn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				v_min_first_change = rs.getLong("v_min_first_change");
				logger.info("v_min_first_change={} ", v_min_first_change);
			}
			rs.close();
			stmt.close();
			
			
			logger.info("logminer add file begin!");
			Long groupNum = null;
			String member = null;
			sql =  "select a.group#, b.member from v$log a inner join v$logfile b on a.group# = b.group# "
				  + " where sequence# >= " 
				  +	"   ( "
				  +	"      select sequence# from v$log " 
				  +	"      where first_change# <= " + v_scn +" and " + v_scn +" < next_change# "
				  +	"    ) "
				  +	"    order by sequence#, member";
			stmt = sourceConn.createStatement();
			rs = stmt.executeQuery(sql);
			
			Long v_group = 0L;
			int v_index = 0;
			while (rs.next()) {
				groupNum = rs.getLong("group#");
				member = rs.getString("member");
				logger.info("groupNum={} ", groupNum);
				logger.info("member={} ", member);
				if (v_group.longValue() != groupNum.longValue()) {
					v_group = groupNum;
					v_index++;
					if (v_index == 1) {
						sql = "begin \ndbms_logmnr.add_logfile(LogFileName=>'" + member + "',Options=>dbms_logmnr.new) \n; end;";
					} else {
						sql = "begin \ndbms_logmnr.add_logfile(LogFileName=>'" + member + "',Options=>dbms_logmnr.addfile) \n; end;";
					}
					cstmt = sourceConn.prepareCall(sql);
					cstmt.execute();
				}
			}
			rs.close();
			stmt.close();
			logger.info("logminer add file done!");
			
			logger.info("logminer startloginer begin!");
			sql = "SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => " + v_scn  + ", " 
    + " OPTIONS =>  SYS.DBMS_LOGMNR.SKIP_CORRUPTION "
    + " + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER "
    + " + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT "
    + " + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG "
    + " + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY "
    + " + SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT)";
			
			logger.info("logminer startloginer done!");
			
			
			logger.info("logminer startloginer begin!");
			
			
			
			logger.info("select v$logmnr_contents begin!");
			Long threadNum;
			Long scn;
			Long commitScn;
			Long startScn;
			String xid;
			Date timestamp;
			Date commitTimestamp;
			Integer operationCode;
			String operation;
			Integer status; 
			String segTypeName;
			String info;
			Integer rollback;
			String segOwner;
			String segName;
			String tableName;
			String username;
			String sqlRedo;
			String rowId;
			Integer csf;
			String tableSpace;
			String sessionInfo;
			String rsId;
			Long rbasqn;
			Long rbablk;
			Long sequenceNum;
			String txName;
			
			sql = "SELECT THREAD#, SCN, COMMIT_SCN, START_SCN, (XIDUSN||'.'||XIDSLT||'.'||XIDSQN) AS XID, TIMESTAMP, COMMIT_TIMESTAMP "
					+ ", OPERATION_CODE, OPERATION, STATUS, SEG_TYPE_NAME, INFO, ROLLBACK, SEG_OWNER, SEG_NAME, TABLE_NAME , USERNAME, SQL_REDO, ROW_ID, CSF "
					+ "   , TABLE_SPACE, SESSION_INFO, RS_ID, RBASQN, RBABLK, SEQUENCE#, TX_NAME "
					+ "   FROM  v$logmnr_contents "
					+ "  WHERE "
					+ "    (OPERATION_CODE in (1,2,3) and COMMIT_SCN >= " + v_commit_scn + " and COMMIT_SCN < " + o_current_scn
					+ "      and "
					+ "       (seg_owner || '.' || table_name) "
					+ "        in ('LS_EBAO.T_LOGMINER_SCN') "
				    + "    ) "
				    + "    order by scn "; 
			
			stmt = sourceConn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				Map<String, Object> data = new HashMap<>();
				threadNum = rs.getLong("THREAD#");
				scn = rs.getLong("SCN");
				commitScn = rs.getLong("COMMIT_SCN");
				startScn = rs.getLong("START_SCN");
				xid = rs.getString("XID");
				timestamp = rs.getDate("TIMESTAMP");
				commitTimestamp = rs.getDate("COMMIT_TIMESTAMP");
				operationCode = rs.getInt("OPERATION_CODE");
				operation = rs.getString("OPERATION");
				status = rs.getInt("STATUS");; 
				segTypeName = rs.getString("SEG_TYPE_NAME");
				info = rs.getString("INFO");
				rollback = rs.getInt("ROLLBACK");
				segOwner = rs.getString("SEG_OWNER");
				segName = rs.getString("SEG_NAME");//
				tableName = rs.getString("TABLE_NAME");
				username = rs.getString("USERNAME");//
				sqlRedo = rs.getString("SQL_REDO");
				rowId = rs.getString("ROW_ID");
				csf = rs.getInt("CSF");
				tableSpace = rs.getString("TABLE_SPACE");//
				sessionInfo = rs.getString("SESSION_INFO");//
				rsId = rs.getString("RS_ID");//
				rbasqn = rs.getLong("RBASQN");//
				rbablk = rs.getLong("RBABLK");//
				sequenceNum = rs.getLong("SEQUENCE#");//
				txName = rs.getString("TX_NAME");//
				
				data.put("threadNum", threadNum);
				data.put("scn", scn);
				data.put("commitScn", commitScn);
				data.put("startScn", startScn);
				data.put("xid", xid);
				data.put("timestamp", timestamp);
				data.put("commitTimestamp", commitTimestamp);
				data.put("operationCode", operationCode);
				data.put("operation", operation);
				data.put("status", status);
				data.put("segTypeName", segTypeName);
				data.put("info", info);
				data.put("rollback",rollback);
				data.put("segOwner", segOwner);
				data.put("segName", segName);
				data.put("tableName", tableName);
				data.put("username", username);
				data.put("sqlRedo", sqlRedo);
				data.put("rowId", rowId);
				data.put("csf", csf);
				data.put("tableSpace", tableSpace);
				data.put("sessionInfo", sessionInfo);
				data.put("rsId", rsId);
				data.put("rbasqn", rbasqn);
				data.put("rbablk",rbablk);
				data.put("sequenceNum",sequenceNum);
				data.put("txName", txName); 
				
				logger.info(">>>>> data={}",data);
			}
			
			logger.info("select v$logmnr_contents done!");
		} finally {
			if (rs != null) rs.close();
			if (stmt != null) stmt.close();
			if (cstmt != null) cstmt.close();
			if (sourceConn != null) sourceConn.close();
		}
	}
	

}
