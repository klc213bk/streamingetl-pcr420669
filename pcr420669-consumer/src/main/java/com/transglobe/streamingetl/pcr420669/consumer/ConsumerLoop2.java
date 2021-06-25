package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.pcr420669.consumer.model.Address;
import com.transglobe.streamingetl.pcr420669.consumer.model.PartyContact;
import com.transglobe.streamingetl.pcr420669.consumer.model.StreamingEtlHealthCdc;

public class ConsumerLoop2 implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(ConsumerLoop2.class);

	private static final Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static final Integer INSURED_LIST_ROLE_TYPE = 2;
	private static final Integer CONTRACT_BENE_ROLE_TYPE = 3;

	private final KafkaConsumer<String, String> consumer;
	private final int id;

	private Config config;

	private BasicDataSource sourceConnPool;
	private BasicDataSource sinkConnPool;

	private String streamingEtlHealthCdcTableName;
	private String policyHolderTableName;
	private String insuredListTableName;
	private String contractBeneTableName;
	private String policyHolderTableNameLog;
	private String insuredListTableNameLog;
	private String contractBeneTableNameLog;
	private String addressTableName;

	private String sourceSyncTableContractMaster;
	private String sourceSyncTablePolicyChange;

	public ConsumerLoop2(int id,
			String groupId,  
			Config config,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool) {
		this.id = id;
		this.config = config;
		this.sourceConnPool = sourceConnPool;
		this.sinkConnPool = sinkConnPool;
		Properties props = new Properties();
		props.put("bootstrap.servers", config.bootstrapServers);
		props.put("group.id", groupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);

		streamingEtlHealthCdcTableName = config.sourceTableStreamingEtlHealthCdc;
		policyHolderTableName = config.sourceTablePolicyHolder;
		insuredListTableName = config.sourceTableInsuredList;
		contractBeneTableName = config.sourceTableContractBene;
		policyHolderTableNameLog = config.sourceTablePolicyHolderLog;
		insuredListTableNameLog = config.sourceTableInsuredListLog;
		contractBeneTableNameLog = config.sourceTableContractBeneLog;
		addressTableName = config.sourceTableAddress;

		sourceSyncTableContractMaster = config.sourceSyncTableContractMaster;
		sourceSyncTablePolicyChange = config.sourceSyncTablePolicyChange;
	}

	@Override
	public void run() {

		try {
			consumer.subscribe(config.topicList);

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				if (records.count() > 0) {
					Connection sinkConn = null;
					Connection sourceConn = null;
					int tries = 0;

					while (sinkConnPool.isClosed()) {
						tries++;
						try {
							sinkConnPool.restart();

							logger.info("   >>> Connection Pool restart, try {} times", tries);

							Thread.sleep(30000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}

					}
					sinkConn = sinkConnPool.getConnection();


					for (ConsumerRecord<String, String> record : records) {
						Map<String, Object> data = new HashMap<>();
						try {	
							sinkConn.setAutoCommit(false);
							data.put("partition", record.partition());
							data.put("offset", record.offset());
							data.put("value", record.value());
							//					System.out.println(this.id + ": " + data);

							ObjectMapper objectMapper = new ObjectMapper();
							objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

							JsonNode jsonNode = objectMapper.readTree(record.value());
							JsonNode payload = jsonNode.get("payload");
							//	payloadStr = payload.toString();

							String operation = payload.get("OPERATION").asText();

							String tableName = payload.get("TABLE_NAME").asText();
							//	logger.info("   >>>operation={}, fullTableName={}", operation, fullTableName);
							if (StringUtils.equals(streamingEtlHealthCdcTableName, tableName)) {
								doHealth(sinkConn, objectMapper, payload);
							} else {
								logger.info("   >>>payload={}", payload.toPrettyString());
							}

							// T 表
							if (StringUtils.equals(policyHolderTableName, tableName)
									|| StringUtils.equals(insuredListTableName, tableName)
									|| StringUtils.equals(contractBeneTableName, tableName)) {

								String payLoadData = payload.get("data").toString();

								PartyContact partyContact = objectMapper.readValue(payLoadData, PartyContact.class);

								partyContact.setMobileTel(StringUtils.trim(partyContact.getMobileTel()));
								partyContact.setEmail(StringUtils.trim(StringUtils.lowerCase(partyContact.getEmail())));
								if (config.sourceTablePolicyHolder.equals(tableName)) {
									partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
								} else if (config.sourceTableInsuredList.equals(tableName)) {
									partyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
								} else if (config.sourceTableContractBene.equals(tableName)) {
									partyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
									partyContact.setEmail(null);// 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
								} 
								
								logger.info("   >>>operation={}", operation);
								logger.info("   >>>partyContact={}", partyContact);

								// check sourceSyncTableContractMaster
								sourceConn = sourceConnPool.getConnection();
								int liabilityState = getLiabilityState(sourceConn, partyContact.getPolicyId());
								logger.info("   >>>liabilityState={}", liabilityState);

								if (liabilityState == 0) {
									// do  同步(Insert/Update/Delete)
									if ("INSERT".equals(operation)) {
										insertPartyContact(sinkConn, partyContact);
									} else if ("UPDATE".equals(operation)) {
										String beforePayLoadData = payload.get("before").toString();
										PartyContact beforePartyContact = objectMapper.readValue(beforePayLoadData, PartyContact.class);
										logger.info("   >>>beforePartyContact={}", beforePartyContact);
										
										if (partyContact.equals(beforePartyContact)) {
											// ignore
										} else {
											updatePartyContact(sourceConn, sinkConn, partyContact, beforePartyContact);
										}
									} else if ("DELETE".equals(operation)) {
										deletePartyContact(sinkConn, partyContact);
									}

								} else {
									// ignore
								}

							} // Log 表
							else if (StringUtils.equals(policyHolderTableNameLog, tableName)
									|| StringUtils.equals(insuredListTableNameLog, tableName)
									|| StringUtils.equals(contractBeneTableNameLog, tableName)) {

								String payLoadData = payload.get("data").toString();

								PartyContact partyContact = objectMapper.readValue(payLoadData, PartyContact.class);
								String lastCmtFlg = partyContact.getLastCmtFlg();
								
								// LAST_CMT_FLG ＝ ʻYʻ 同步(Insert/update)
								if (StringUtils.equals("Y", lastCmtFlg)) {
									if ("INSERT".equals(operation)) {
										insertPartyContact(sinkConn, partyContact);
									} else if ("UPDATE".equals(operation)) {
										String beforePayLoadData = payload.get("before").toString();
										PartyContact beforePartyContact = objectMapper.readValue(beforePayLoadData, PartyContact.class);
										logger.info("   >>>Log beforePartyContact={}", beforePartyContact);
										
										if (partyContact.equals(beforePartyContact)) {
											// ignore
										} else {
											updatePartyContact(sourceConn, sinkConn, partyContact, beforePartyContact);
										}	
									}
								} 
								// LAST_CMT_FLG ＝ ʻNʻ 且 t_policy_change.policy_chg_status ＝2 同步 (Delete)
								else {
									Long policyChgId = partyContact.getPolicyChgId();
									int policyChgStatus = getPolicyChangeStatus(sourceConn, policyChgId);
									
									// delete
									if (policyChgStatus == 2) {
										deletePartyContact(sinkConn, partyContact);
									} else {
										// ignore
									}
								}
								
								
							} // Address 表
							else if (StringUtils.equals(addressTableName, tableName)) {
								String payLoadData = payload.get("data").toString();
								Address address = objectMapper.readValue(payLoadData, Address.class);
								if ("INSERT".equals(operation)) {
									insertAddress(sinkConn, address);
								} else if ("UPDATE".equals(operation)) {
									String beforePayLoadData = payload.get("before").toString();
									Address beforeAddress = objectMapper.readValue(beforePayLoadData, Address.class);
									logger.info("   >>>beforeAddress={}", beforeAddress);
									if (address.equals(beforeAddress)) {
										// ignore
									} else {
										updateAddress(sinkConn, beforeAddress, address);
									}	
									
								} else if ("DELETE".equals(operation)) {
									deleteAddress(sinkConn, address);
								}
							} else {
								throw new Exception(">>> Error: no such table name:" + tableName);
							}

							sinkConn.commit();
						} catch(Exception e) {
							logger.error(">>>message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e), data);
						}
					}
					if (sourceConn != null && !sourceConn.isClosed()) sourceConn.close();
					if (sinkConn != null && !sinkConn.isClosed()) sinkConn.close();
				}


			}
		} catch (Exception e) {
			// ignore for shutdown 
			logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			consumer.close();

			if (sinkConnPool != null) {
				try {
					sinkConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
		}
	}

	public void shutdown() {
		consumer.wakeup();
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
			pstmt.setString(3, "Logminer-1");
			pstmt.setTimestamp(4, new java.sql.Timestamp(logminerTime));
			pstmt.setString(5, "pcr420669" + "-" + id);
			pstmt.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));

			pstmt.executeUpdate();
			pstmt.close();

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
			sql = "select POLICY_CHG_STATUS from " + this.sourceSyncTablePolicyChange + " where POLICY_ID = ?";
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
	private String getSourceAddress1(Connection sourceConn, Long addressId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String address1 = null;
		try {
			sql = "select ADDRESS_1 from " + this.addressTableName + " where ADDRESS_ID = ?";
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

	private void insertPartyContact(Connection sinkConn, PartyContact partyContact) throws Exception  {
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
					String address1 = getSourceAddress1(sinkConn, partyContact.getAddressId());
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
	private void deletePartyContact(Connection sinkConn, PartyContact partyContact) throws Exception  {
		PreparedStatement pstmt = null;
		try {
			String sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact 
					+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
			int count = getCount(sinkConn, sql);
			if (count > 0) {
				sql = "delete from " + config.sinkTablePartyContact + " where role_type = ? and list_id = ?";

				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setInt(1, partyContact.getRoleType());
				pstmt.setLong(2, partyContact.getListId());
				
				pstmt.executeUpdate();
				pstmt.close();

			} else {
				// record exists, error
				String error = String.format("table=%s record does not exist, therefore cannot delete, role_type=%d, list_id=%d", config.sinkTablePartyContact, partyContact.getRoleType(), partyContact.getListId());
				throw new Exception(error);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (pstmt != null) pstmt.close();
		}
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

	private Integer getCount(Connection conn, String sql) throws SQLException {

		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet resultSet = pstmt.executeQuery();
		Integer count = 0; 
		while (resultSet.next()) {
			count = resultSet.getInt("COUNT");
		}
		resultSet.close();
		pstmt.close();

		return count;
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
					+ " set ADDRESS_1 = null where ADDRESS_ID = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
}