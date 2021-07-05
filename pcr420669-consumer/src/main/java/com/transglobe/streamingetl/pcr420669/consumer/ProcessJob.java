package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.consumer.model.Address;
import com.transglobe.streamingetl.pcr420669.consumer.model.PartyContact;
import com.transglobe.streamingetl.pcr420669.consumer.model.StreamingEtlHealthCdc;

public class ProcessJob implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(ProcessJob.class);

	private static final Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static final Integer INSURED_LIST_ROLE_TYPE = 2;
	private static final Integer CONTRACT_BENE_ROLE_TYPE = 3;

	private ConsumerRecord<String, String> record;
	private BasicDataSource sourceConnPool;
	private BasicDataSource sinkConnPool;

	private Config config;

	private String policyHolderTableName;
	private String insuredListTableName;
	private String contractBeneTableName;
	private String policyHolderTableNameLog;
	private String insuredListTableNameLog;
	private String contractBeneTableNameLog;
	private String addressTableName;

	private String streamingEtlHealthCdcTableName;

	private String sourceSyncTableAddress;
	private String sourceSyncTableContractMaster;
	private String sourceSyncTablePolicyChange;

	public ProcessJob(ConsumerRecord<String, String> record, BasicDataSource sourceConnPool
			, BasicDataSource sinkConnPool, Config config) {
		this.record = record;
		this.sourceConnPool = sourceConnPool;
		this.sinkConnPool = sinkConnPool;
		this.config = config;

		streamingEtlHealthCdcTableName = config.sourceTableStreamingEtlHealthCdc;
		policyHolderTableName = config.sourceTablePolicyHolder;
		insuredListTableName = config.sourceTableInsuredList;
		contractBeneTableName = config.sourceTableContractBene;
		policyHolderTableNameLog = config.sourceTablePolicyHolderLog;
		insuredListTableNameLog = config.sourceTableInsuredListLog;
		contractBeneTableNameLog = config.sourceTableContractBeneLog;
		addressTableName = config.sourceTableAddress;

		sourceSyncTableAddress = config.sourceSyncTableAddress;
		sourceSyncTableContractMaster = config.sourceSyncTableContractMaster;
		sourceSyncTablePolicyChange = config.sourceSyncTablePolicyChange;
	}

	@Override
	public void run() {
		Map<String, Object> data = new HashMap<>();
		Connection sourceConn = null;
		Connection sinkConn = null;
		try {
			sourceConn = sourceConnPool.getConnection();
			sinkConn = sinkConnPool.getConnection();

			sinkConn.setAutoCommit(false);
			data.put("partition", record.partition());
			data.put("offset", record.offset());
			data.put("value", record.value());

			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

			JsonNode jsonNode = objectMapper.readTree(record.value());
			JsonNode payload = jsonNode.get("payload");

			String operation = payload.get("OPERATION").asText();

			String tableName = payload.get("TABLE_NAME").asText();
			logger.info("   >>>offset={},operation={}, TableName={}", record.offset(), operation, tableName);

			boolean isTtable = false;
			boolean isTlogtable = false;
			if (StringUtils.equals(policyHolderTableName, tableName)
					|| StringUtils.equals(insuredListTableName, tableName)
					|| StringUtils.equals(contractBeneTableName, tableName) ) {
				isTtable = true;		
			} else if (StringUtils.equals(policyHolderTableNameLog, tableName)
					|| StringUtils.equals(insuredListTableNameLog, tableName)
					|| StringUtils.equals(contractBeneTableNameLog, tableName) ) {
				isTlogtable = true;
			}

			PartyContact partyContact = null;
			PartyContact beforePartyContact = null;
			if (isTtable || isTlogtable) {
				logger.info("   >>>payload={}", payload.toPrettyString());
				String payLoadData = payload.get("data").toString();
				String beforePayLoadData = payload.get("before").toString();
				partyContact = (payLoadData == null)? null : objectMapper.readValue(payLoadData, PartyContact.class);;
				beforePartyContact = (beforePayLoadData == null)? null : objectMapper.readValue(beforePayLoadData.toString(), PartyContact.class);;

				if (partyContact != null) { 
					partyContact.setMobileTel(StringUtils.trim(partyContact.getMobileTel()));
					partyContact.setEmail(StringUtils.trim(StringUtils.lowerCase(partyContact.getEmail())));
					if (policyHolderTableName.equals(tableName)
							|| policyHolderTableNameLog.equals(tableName)) {
						partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
					} else if (insuredListTableName.equals(tableName)
							|| insuredListTableNameLog.equals(tableName)) {
						partyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
					} else if (contractBeneTableName.equals(tableName)
							|| contractBeneTableNameLog.equals(tableName)) {
						partyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
						partyContact.setEmail(null);// 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
					} 

				}
				if (beforePartyContact != null) { 
					beforePartyContact.setMobileTel(StringUtils.trim(beforePartyContact.getMobileTel()));
					beforePartyContact.setEmail(StringUtils.trim(StringUtils.lowerCase(beforePartyContact.getEmail())));
					if (policyHolderTableName.equals(tableName)
							|| policyHolderTableNameLog.equals(tableName)) {
						beforePartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
					} else if (insuredListTableName.equals(tableName)
							|| insuredListTableNameLog.equals(tableName)) {
						beforePartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
					} else if (contractBeneTableName.equals(tableName)
							|| contractBeneTableNameLog.equals(tableName)) {
						beforePartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
						beforePartyContact.setEmail(null);// 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
					} 

				}			
				logger.info("   >>>partyContact={}", ((partyContact == null)? null : ToStringBuilder.reflectionToString(partyContact)));
				logger.info("   >>>beforepartyContact={}", ((beforePartyContact == null)? null : ToStringBuilder.reflectionToString(beforePartyContact)));

			} 
			// T 表
			if (isTtable) {
				// check sourceSyncTableContractMaster
				int liabilityState = (partyContact != null)? getLiabilityState(sourceConn, partyContact.getPolicyId())
						: getLiabilityState(sourceConn, beforePartyContact.getPolicyId());
				logger.info("   >>>liabilityState={}", liabilityState);

				if (liabilityState == 0) {
					// do  同步(Insert/Update/Delete)
					if ("INSERT".equals(operation)) {
						insertPartyContact(sourceConn, sinkConn, partyContact);
					} else if ("UPDATE".equals(operation)) {
						if (partyContact.equals(beforePartyContact)) {
							// ignore
						} else {
							updatePartyContact(sourceConn, sinkConn, partyContact, beforePartyContact);
						}
					} else if ("DELETE".equals(operation)) {
						deletePartyContact(sinkConn, beforePartyContact);
					}

				} else {
					// ignore
				}

			} // Log 表
			else if (isTlogtable) {
				String lastCmtFlg = (partyContact != null)? partyContact.getLastCmtFlg()
						: beforePartyContact.getLastCmtFlg();

				// LAST_CMT_FLG ＝ ʻYʻ 同步(Insert/update)
				if ("INSERT".equals(operation) ) {
					if (StringUtils.equals("Y", lastCmtFlg)) {
						insertPartyContact(sourceConn, sinkConn, partyContact);
					}
				} else if ("UPDATE".equals(operation)) {
					if (StringUtils.equals("Y", lastCmtFlg)) {
						if (partyContact.equals(beforePartyContact)) {
							// ignore
						} else {
							updatePartyContact(sourceConn, sinkConn, partyContact, beforePartyContact);
						}	
					} 
					// LAST_CMT_FLG ＝ ʻNʻ 且 t_policy_change.policy_chg_status ＝2 同步 (Delete)
					else if (StringUtils.equals("N", lastCmtFlg)) {
						Long policyChgId = beforePartyContact.getPolicyChgId();
						int policyChgStatus = getPolicyChangeStatus(sourceConn, policyChgId);

						// delete
						if (policyChgStatus == 2) {
							deletePartyContact(sinkConn, beforePartyContact);
						} else {
							// ignore
						}
					}
				}
			} // Address 表
			else if (StringUtils.equals(addressTableName, tableName)) {
				String payLoadData = payload.get("data").toString();
				String beforePayLoadData = payload.get("before").toString();
				Address address = (payLoadData == null)? null : objectMapper.readValue(payLoadData, Address.class);
				Address beforeAddress = (beforePayLoadData == null)? null : objectMapper.readValue(beforePayLoadData, Address.class);

				logger.info("   >>>address={}", ((address == null)? null : ToStringBuilder.reflectionToString(address)));
				logger.info("   >>>beforeAddress={}", ((beforeAddress == null)? null : ToStringBuilder.reflectionToString(beforeAddress)));

				if ("INSERT".equals(operation)) {
					insertAddress(sinkConn, address);
				} else if ("UPDATE".equals(operation)) {

					if (address.equals(beforeAddress)) {
						// ignore
					} else {
						updateAddress(sinkConn, beforeAddress, address);
					}	

				} else if ("DELETE".equals(operation)) {
					deleteAddress(sinkConn, beforeAddress);
				}
			} else if (StringUtils.equals(streamingEtlHealthCdcTableName, tableName)) {
				doHealth(sinkConn, objectMapper, payload);
			} else {
				throw new Exception(">>> Error: no such table name:" + tableName);
			}

			sinkConn.commit();

		} catch (Exception e) {
			logger.error(">>>Consumer error, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (Exception e) {
					logger.error(">>>sourceConn.close Error:" + ExceptionUtils.getStackTrace(e));
				}
			}
			if (sinkConn != null) {
				try {
					sinkConn.close();
				} catch (Exception e) {
					logger.error(">>>sinkConn.close Error:" + ExceptionUtils.getStackTrace(e));
				}
			}
		}

	}
	private void doHealth(Connection conn, ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String data = payload.get("data").toString();
		Long logminerTime = Long.valueOf(payload.get("TIMESTAMP").toString());
		StreamingEtlHealthCdc healthCdc = objectMapper.readValue(data, StreamingEtlHealthCdc.class);

		insertStreamingEtlHealth(conn, healthCdc, logminerTime);

	}
	private void insertStreamingEtlHealth(Connection conn, StreamingEtlHealthCdc healthSrc, long logminerTime) throws Exception {

		String sql = null;
		PreparedStatement pstmt = null;
		try {
			sql = "insert into " + config.sinkTableStreamingEtlHealth 
					+ " (id,cdc_time,logminer_id,logminer_time,consumer_id,consumer_time) "
					+ " values (?, ?, ?, ?, ? ,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, System.currentTimeMillis());
			pstmt.setTimestamp(2, new java.sql.Timestamp(healthSrc.getCdctime()));
			pstmt.setString(3, "Logminer");
			pstmt.setTimestamp(4, new java.sql.Timestamp(logminerTime));
			pstmt.setString(5, "pcr420669");
			pstmt.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));

			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}

	}
	private Integer getLiabilityState(Connection sourceConn, Long policyId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Integer liabilityState = null;
		try {
			sql = "select LIABILITY_STATE from " + this.sourceSyncTableContractMaster + " where POLICY_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, policyId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				liabilityState = rs.getInt("LIABILITY_STATE");
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return liabilityState;
	}
	private void insertPartyContact(Connection sourceConn, Connection sinkConn, PartyContact partyContact) throws Exception  {
		PreparedStatement pstmt = null;
		try {
			String sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact 
					+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
			int count = getCount(sinkConn, sql);
			if (count == 0) {
				if (partyContact.getAddressId() == null) {
					sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL) " 
							+ " values (?,?,?,?,?,?,?)";
					pstmt = sinkConn.prepareStatement(sql);
					pstmt.setInt(1, partyContact.getRoleType());
					pstmt.setLong(2, partyContact.getListId());
					pstmt.setLong(3, partyContact.getPolicyId());
					pstmt.setString(4, partyContact.getName());
					pstmt.setString(5, partyContact.getCertiCode());
					pstmt.setString(6, partyContact.getMobileTel());
					if (partyContact.getRoleType() == CONTRACT_BENE_ROLE_TYPE) {
						pstmt.setNull(7, Types.VARCHAR);
					} else {
						pstmt.setString(7, partyContact.getEmail());
					}

					pstmt.executeUpdate();
					pstmt.close();
				} else {
					String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());
					partyContact.setAddress1(address1);

					sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
							+ " values (?,?,?,?,?,?,?,?,?)";
					pstmt = sinkConn.prepareStatement(sql);
					pstmt.setInt(1, partyContact.getRoleType());
					pstmt.setLong(2, partyContact.getListId());
					pstmt.setLong(3, partyContact.getPolicyId());
					pstmt.setString(4, partyContact.getName());
					pstmt.setString(5, partyContact.getCertiCode());
					pstmt.setString(6, partyContact.getMobileTel());
					pstmt.setString(7, partyContact.getEmail());
					pstmt.setLong(8, partyContact.getAddressId());
					if (address1 == null) {
						pstmt.setNull(9, Types.VARCHAR);
					} else {
						pstmt.setString(9, partyContact.getAddress1());
					}
					pstmt.executeUpdate();
					pstmt.close();
				}

			} else {
				// record exists, error
				String error = String.format("table=%s record already exists, therefore cannot insert, role_type=%d, list_id=%d", config.sinkTablePartyContact, partyContact.getRoleType(), partyContact.getListId());
				throw new Exception(error);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private String getSourceAddress1(Connection sourceConn, Long addressId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String address1 = null;
		try {
			sql = "select ADDRESS_1 from " + this.sourceSyncTableAddress + " where ADDRESS_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, addressId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				address1 = rs.getString("ADDRESS_1");
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return address1;
	}
	private Integer getCount(Connection conn, String sql) throws SQLException {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			Integer count = 0; 
			while (rs.next()) {
				count = rs.getInt("COUNT");
			}
			rs.close();
			pstmt.close();

			return count;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}


	}
	private void updatePartyContact(Connection sourceConn, Connection sinkConn, PartyContact partyContact, PartyContact beforePartyContact) throws Exception  {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			String sql = "select *  from " + config.sinkTablePartyContact 
					+ " where role_type = ? and list_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setInt(1, partyContact.getRoleType());
			pstmt.setLong(2, partyContact.getListId());
			rs = pstmt.executeQuery();
			//Long partyAddressId = null;
			int count = 0;
			while (rs.next()) {
				//	partyAddressId = rs.getLong("ADDRESS_ID");
				count++;
			}
			rs.close();
			pstmt.close();
			logger.info(">>>count={}", count);

			if (count > 0) {
				if (partyContact.getAddressId() == null) {
					sql = "update " + config.sinkTablePartyContact 
							+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=null,ADDRESS_1=null"
							+ " where ROLE_TYPE=? and LIST_ID=?";
					pstmt = sinkConn.prepareStatement(sql);
					pstmt.setLong(1, partyContact.getPolicyId());
					pstmt.setString(2, partyContact.getName());
					pstmt.setString(3, partyContact.getCertiCode());
					pstmt.setString(4, partyContact.getMobileTel());
					pstmt.setString(5, partyContact.getEmail());

					pstmt.setInt(6, partyContact.getRoleType());
					pstmt.setLong(7, partyContact.getListId());

					pstmt.executeUpdate();
					pstmt.close();
				} else {

					// update without address
					if (beforePartyContact.getAddressId().longValue() == partyContact.getAddressId().longValue()) {
						sql = "update " + config.sinkTablePartyContact 
								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?"
								+ " where ROLE_TYPE=? and LIST_ID=?";
						pstmt = sinkConn.prepareStatement(sql);
						pstmt.setLong(1, partyContact.getPolicyId());
						pstmt.setString(2, partyContact.getName());
						pstmt.setString(3, partyContact.getCertiCode());
						pstmt.setString(4, partyContact.getMobileTel());
						pstmt.setString(5, partyContact.getEmail());

						pstmt.setInt(6, partyContact.getRoleType());
						pstmt.setLong(7, partyContact.getListId());

						pstmt.executeUpdate();
						pstmt.close();
					} // update with address
					else {
						// get address1
						String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());	
						partyContact.setAddress1(address1);

						// update 
						sql = "update " + config.sinkTablePartyContact 
								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=?"
								+ " where ROLE_TYPE=? and LIST_ID=?";
						pstmt = sinkConn.prepareStatement(sql);
						pstmt.setLong(1, partyContact.getPolicyId());
						pstmt.setString(2, partyContact.getName());
						pstmt.setString(3, partyContact.getCertiCode());
						pstmt.setString(4, partyContact.getMobileTel());
						pstmt.setString(5, partyContact.getEmail());
						pstmt.setLong(6, partyContact.getAddressId());
						pstmt.setString(7, partyContact.getAddress1());

						pstmt.setInt(8, partyContact.getRoleType());
						pstmt.setLong(9, partyContact.getListId());

						pstmt.executeUpdate();
						pstmt.close();
					}

				}

			} else {
				// record exists, error
				String error = String.format("table=%s record does not exists, therefore cannot update, role_type=%d, list_id=%d", config.sinkTablePartyContact, partyContact.getRoleType(), partyContact.getListId());
				throw new Exception(error);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}
	private void deletePartyContact(Connection sinkConn, PartyContact beforePartyContact) throws Exception  {
		PreparedStatement pstmt = null;
		try {
			String sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact 
					+ " where role_type = " + beforePartyContact.getRoleType() + " and list_id = " + beforePartyContact.getListId();
			int count = getCount(sinkConn, sql);
			if (count > 0) {
				sql = "delete from " + config.sinkTablePartyContact + " where role_type = ? and list_id = ?";

				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setInt(1, beforePartyContact.getRoleType());
				pstmt.setLong(2, beforePartyContact.getListId());

				pstmt.executeUpdate();
				pstmt.close();

			} else {
				// record exists, error
				String error = String.format("table=%s record does not exist, therefore cannot delete, role_type=%d, list_id=%d", config.sinkTablePartyContact, beforePartyContact.getRoleType(), beforePartyContact.getListId());
				throw new Exception(error);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private Integer getPolicyChangeStatus(Connection sourceConn, Long policyChgId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Integer policyChgStatus = null;
		try {
			sql = "select POLICY_CHG_STATUS from " + this.sourceSyncTablePolicyChange + " where POLICY_CHG_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, policyChgId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				policyChgStatus = rs.getInt("POLICY_CHG_STATUS");
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return policyChgStatus;
	}
	private void insertAddress(Connection sinkConn, Address address) throws Exception {

		String sql = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			sql = "select ROLE_TYPE,LIST_ID from " + config.sinkTablePartyContact + " where address_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());
			rs = pstmt.executeQuery();
			int count = 0;
			while (rs.next()) {
				count++;
				break;

			}
			rs.close();
			pstmt.close();

			if (count == 0) {
				// ignore
			} else {
				// update party contact
				sql = "update "  + config.sinkTablePartyContact + " set address_1 = ? where address_id = ?";
				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setString(1, StringUtils.trim(address.getAddress1()));
				pstmt.setLong(2, address.getAddressId());
				pstmt.executeUpdate();
				pstmt.close();
			}

		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}

	}
	private void updateAddress(Connection conn, Address oldAddress, Address newAddress) throws Exception  {

		PreparedStatement pstmt = null;
		String sql = "";
		try {
			// update PartyContact
			sql = "update " + config.sinkTablePartyContact
					+ " set ADDRESS_1 = ? where ADDRESS_ID = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, StringUtils.trim(newAddress.getAddress1()));
			pstmt.setLong(2, oldAddress.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}

	private void deleteAddress(Connection sinkConn, Address address) throws Exception  {

		PreparedStatement pstmt = null;
		String sql = "";
		try {
			// update PartyContact
			sql = "update " + config.sinkTablePartyContact
					+ " set ADDRESS_ID = null;ADDRESS_1 = null where ADDRESS_ID = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
}
