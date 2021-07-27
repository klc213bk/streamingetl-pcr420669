package com.transglobe.streamingetl.pcr420669.consumer;

import java.io.Console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import com.transglobe.streamingetl.pcr420669.consumer.model.LogminerScnSink;
import com.transglobe.streamingetl.pcr420669.consumer.model.PartyContact;

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

	private String policyHolderTableName;
	private String insuredListTableName;
	private String contractBeneTableName;
	private String policyHolderTableNameLog;
	private String insuredListTableNameLog;
	private String contractBeneTableNameLog;
	private String addressTableName;

	private String sourceSyncTableAddress;
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
		props.put("client.id", groupId + "-" + id );
		props.put("group.instance.id", groupId + "-mygid" );
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("session.timeout.ms", 60000 ); // 60 seconds
		props.put("max.poll.records", 50 );
		this.consumer = new KafkaConsumer<>(props);

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

		try {
			consumer.subscribe(config.topicList);

			logger.info("   >>>>>>>>>>>>>>>>>>>>>>>> run ..........");

			while (true) {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

				if (records.count() > 0) {
					
					//Connection sinkConn = null;
					//Connection sourceConn = null;
					int tries = 0;
					while (sourceConnPool.isClosed()) {
						tries++;
						try {
							sourceConnPool.restart();

							logger.info("   >>> Source Connection Pool restart, try {} times", tries);

							Thread.sleep(10000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}
					}
					tries = 0;
					while (sinkConnPool.isClosed()) {
						tries++;
						try {
							sinkConnPool.restart();

							logger.info("   >>> Connection Pool restart, try {} times", tries);

							Thread.sleep(10000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}

					}


					for (ConsumerRecord<String, String> record : records) {
						Map<String, Object> data = new HashMap<>();

						Connection sourceConn = null;
						Connection sinkConn = null;
						PreparedStatement sinkPstmt = null;
						ResultSet sinkRs = null;
						try {	
							sourceConn = sourceConnPool.getConnection();
							sinkConn = sinkConnPool.getConnection();
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
							Long scn = Long.valueOf(payload.get("SCN").asText());
							String rsId = payload.get("RS_ID").asText();
							Long ssn = Long.valueOf(payload.get("SSN").asText());
							String sqlRedo = payload.get("SQL_REDO").toString();
							logger.info("   >>>offset={},operation={}, TableName={}, scn={}, rsId={}, ssn={}", record.offset(), operation, tableName, scn, rsId, ssn);

							
							// query T_SUPPL_LOG_SYNC
							String sql = "select * from " + config.sinkTableSupplLogSync 
									+ " where RS_ID=? and SSN=?";
							sinkPstmt = sinkConn.prepareStatement(sql);
							sinkPstmt.setString(1, rsId);
							sinkPstmt.setLong(2, scn);
							
							sinkRs = sinkPstmt.executeQuery();
							boolean synExists = false;
							while (sinkRs.next()) {
								synExists = true;
								break;
							}
							
							if (synExists) {
								throw new Exception(">>> Skip this record. Record could be duplicated because rsId:" + rsId + " and ssn:" + ssn + " exists.");
							}
							
							// 
							
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
									if (config.sourceTablePolicyHolder.equals(tableName)
											|| config.sourceTablePolicyHolderLog.equals(tableName)) {
										partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
									} else if (config.sourceTableInsuredList.equals(tableName)
											|| config.sourceTableInsuredListLog.equals(tableName)) {
										partyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
									} else if (config.sourceTableContractBene.equals(tableName)
											|| config.sourceTableContractBeneLog.equals(tableName)) {
										partyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
										partyContact.setEmail(null);// 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
									} 

								}
								if (beforePartyContact != null) { 
									beforePartyContact.setMobileTel(StringUtils.trim(beforePartyContact.getMobileTel()));
									beforePartyContact.setEmail(StringUtils.trim(StringUtils.lowerCase(beforePartyContact.getEmail())));
									if (config.sourceTablePolicyHolder.equals(tableName)
											|| config.sourceTablePolicyHolderLog.equals(tableName)) {
										beforePartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
									} else if (config.sourceTableInsuredList.equals(tableName)
											|| config.sourceTableInsuredListLog.equals(tableName)) {
										beforePartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
									} else if (config.sourceTableContractBene.equals(tableName)
											|| config.sourceTableContractBeneLog.equals(tableName)) {
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
								Integer liabilityState = (partyContact != null)? getLiabilityState(sourceConn, partyContact.getPolicyId())
										: getLiabilityState(sourceConn, beforePartyContact.getPolicyId());
								logger.info("   >>>liabilityState={}", liabilityState);

								if (liabilityState != null && liabilityState == 0) {
									// do  同步(Insert/Update/Delete)
									if ("INSERT".equals(operation)) {
										logger.info("   >>>insert ...");
										insertPartyContact(sourceConn, sinkConn, partyContact);
									} else if ("UPDATE".equals(operation)) {
										if (partyContact.equals(beforePartyContact)) {
											// ignore
											logger.info("   >>>ignore, equal ...");
										} else {
											logger.info("   >>>update ...");
											updatePartyContact(sourceConn, sinkConn, partyContact, beforePartyContact);
										}
									} else if ("DELETE".equals(operation)) {
										logger.info("   >>>delete ...");
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
										logger.info("   >>>insert ...");
										insertPartyContact(sourceConn, sinkConn, partyContact);
									}
								} else if ("UPDATE".equals(operation)) {
									if (StringUtils.equals("Y", lastCmtFlg)) {
										if (partyContact.equals(beforePartyContact)) {
											// ignore
											logger.info("   >>>ignore, equal ...");
										} else {
											logger.info("   >>>update ...");
											updatePartyContact(sourceConn, sinkConn, partyContact, beforePartyContact);
										}	
									} 
									// LAST_CMT_FLG ＝ ʻNʻ 且 t_policy_change.policy_chg_status ＝2 同步 (Delete)
									else if (StringUtils.equals("N", lastCmtFlg)) {
										Long policyChgId = beforePartyContact.getPolicyChgId();
										int policyChgStatus = getPolicyChangeStatus(sourceConn, policyChgId);
										logger.info("   >>>policyChgStatus={}", policyChgStatus);

										// delete
										if (policyChgStatus == 2) {
											// check if T 表 exists
											boolean exists = checkExists(sourceConn, beforePartyContact.getRoleType(), beforePartyContact.getListId());
											logger.info("   >>>T 表 exists={}, role_type={}, listId={}", exists, beforePartyContact.getRoleType(), beforePartyContact.getListId());

											if (!exists) {
												logger.info("   >>> delete ...");
												deletePartyContact(sinkConn, beforePartyContact);
											}
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
									logger.info("   >>>insert ...");
									insertAddress(sinkConn, address);
								} else if ("UPDATE".equals(operation)) {

									if (address.equals(beforeAddress)) {
										// ignore
										logger.info("   >>>ignore, equal ...");
									} else {
										logger.info("   >>>update ...");
										updateAddress(sinkConn, beforeAddress, address);
									}	

								} else if ("DELETE".equals(operation)) {
									logger.info("   >>>delete ...");
									deleteAddress(sinkConn, beforeAddress);
								}
							} else if (StringUtils.equals(config.logminerTableLogminerScn, tableName)) {
								logger.info("   >>> operations on {}", tableName);
							} else {
								throw new Exception(">>> Error: no such table name:" + tableName);
							}

							long t = System.currentTimeMillis();
							logger.info("   >>>insertSupplLogSync, rsId={}, ssn={}, scn={}, time={}", rsId, ssn, scn, t);
							sqlRedo = StringUtils.substring(sqlRedo, 0, 1000);
							insertSupplLogSync(sinkConn, rsId, ssn, scn, t, sqlRedo);
							
							sinkConn.commit();
							
						} catch(Exception e) {
							logger.error(">>>record error, message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e), data);
						} finally {
							if (sinkRs != null) sinkRs.close();
							if (sinkPstmt != null) sinkPstmt.close();
							if (sinkConn != null) sinkConn.close();
							if (sourceConn != null) sourceConn.close();
						}
					}

				}


			}
		} catch (Exception e) {
			// ignore for shutdown 
			logger.error(">>>Consumer error, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			consumer.close();

			if (sourceConnPool != null) {
				try {
					sourceConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>sourceConnPool close error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
			if (sinkConnPool != null) {
				try {
					sinkConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>sinkConnPool error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	private boolean checkExists(Connection sourceConn, Integer roleType, Long listId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean exists = false;
		try {
			String table = "";
			if (POLICY_HOLDER_ROLE_TYPE.equals(roleType) ) {
				table = this.policyHolderTableName;
			} else if (INSURED_LIST_ROLE_TYPE.equals(roleType) ) {
				table = this.insuredListTableName;
			} if (CONTRACT_BENE_ROLE_TYPE.equals(roleType) ) {
				table = this.contractBeneTableName;
			}
			sql = "select * from " + table + " where LIST_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, listId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				exists = true;
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return exists;
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

	private void insertPartyContact(Connection sourceConn, Connection sinkConn, PartyContact partyContact) throws Exception  {
		PreparedStatement pstmt = null;
		try {
			String sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact 
					+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
			int count = getCount(sinkConn, sql);
			if (count == 0) {
				long t = System.currentTimeMillis();
				if (partyContact.getAddressId() == null) {
					sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1,INSERT_TIMESTAMP,UPDATE_TIMESTAMP) " 
							+ " values (?,?,?,?,?,?,?,?,?,?,?)";
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
					pstmt.setNull(8, Types.BIGINT);
					pstmt.setNull(9, Types.VARCHAR);
					pstmt.setTimestamp(10, new Timestamp(t));
					pstmt.setTimestamp(11, new Timestamp(t));

					pstmt.executeUpdate();
					pstmt.close();
				} else {
					String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());
					partyContact.setAddress1(address1);

					sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1,INSERT_TIMESTAMP,UPDATE_TIMESTAMP) " 
							+ " values (?,?,?,?,?,?,?,?,?,?,?)";
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
					pstmt.setTimestamp(10, new Timestamp(t));
					pstmt.setTimestamp(11, new Timestamp(t));

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

			long t = System.currentTimeMillis();
			if (count > 0) {
				if (partyContact.getAddressId() == null) {
					sql = "update " + config.sinkTablePartyContact 
							+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=null,ADDRESS_1=null,UPDATE_TIMESTAMP=?"
							+ " where ROLE_TYPE=? and LIST_ID=?";
					pstmt = sinkConn.prepareStatement(sql);
					pstmt.setLong(1, partyContact.getPolicyId());
					pstmt.setString(2, partyContact.getName());
					pstmt.setString(3, partyContact.getCertiCode());
					pstmt.setString(4, partyContact.getMobileTel());
					pstmt.setString(5, partyContact.getEmail());
					pstmt.setTimestamp(6, new Timestamp(t));
					pstmt.setInt(7, partyContact.getRoleType());
					pstmt.setLong(8, partyContact.getListId());

					pstmt.executeUpdate();
					pstmt.close();
				} else {

					// update without address
					if (beforePartyContact.getAddressId() != null 
							&& partyContact.getAddressId() != null
							&& beforePartyContact.getAddressId().longValue() == partyContact.getAddressId().longValue()) {
						sql = "update " + config.sinkTablePartyContact 
								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,UPDATE_TIMESTAMP=?"
								+ " where ROLE_TYPE=? and LIST_ID=?";
						pstmt = sinkConn.prepareStatement(sql);
						pstmt.setLong(1, partyContact.getPolicyId());
						pstmt.setString(2, partyContact.getName());
						pstmt.setString(3, partyContact.getCertiCode());
						pstmt.setString(4, partyContact.getMobileTel());
						pstmt.setString(5, partyContact.getEmail());
						pstmt.setTimestamp(6, new Timestamp(t));
						pstmt.setInt(7, partyContact.getRoleType());
						pstmt.setLong(8, partyContact.getListId());

						pstmt.executeUpdate();
						pstmt.close();
					} // update with address
					else {
						// get address1
						String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());	
						partyContact.setAddress1(address1);

						// update 
						sql = "update " + config.sinkTablePartyContact 
								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=?,UPDATE_TIMESTAMP=?"
								+ " where ROLE_TYPE=? and LIST_ID=?";
						pstmt = sinkConn.prepareStatement(sql);
						pstmt.setLong(1, partyContact.getPolicyId());
						pstmt.setString(2, partyContact.getName());
						pstmt.setString(3, partyContact.getCertiCode());
						pstmt.setString(4, partyContact.getMobileTel());
						pstmt.setString(5, partyContact.getEmail());
						pstmt.setLong(6, partyContact.getAddressId());
						pstmt.setString(7, partyContact.getAddress1());
						pstmt.setTimestamp(8, new Timestamp(t));
						pstmt.setInt(9, partyContact.getRoleType());
						pstmt.setLong(10, partyContact.getListId());

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
				long t = System.currentTimeMillis();
				// update party contact
				sql = "update "  + config.sinkTablePartyContact + " set address_1 = ?,UPDATE_TIMESTAMP=? where address_id = ?";
				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setString(1, StringUtils.trim(address.getAddress1()));
				pstmt.setTimestamp(2, new Timestamp(t));
				pstmt.setLong(3, address.getAddressId());
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
			long t = System.currentTimeMillis();
			// update PartyContact
			sql = "update " + config.sinkTablePartyContact
					+ " set ADDRESS_1 = ?,UPDATE_TIMESTAMP=? where ADDRESS_ID = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, StringUtils.trim(newAddress.getAddress1()));
			pstmt.setTimestamp(2, new Timestamp(t));
			pstmt.setLong(3, oldAddress.getAddressId());

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
			long t = System.currentTimeMillis();
			// update PartyContact
			sql = "update " + config.sinkTablePartyContact
					+ " set ADDRESS_ID = null,ADDRESS_1 = null,UPDATE_TIMESTAMP=? where ADDRESS_ID = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setTimestamp(1, new Timestamp(t));
			pstmt.setLong(2, address.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private void insertSupplLogSync(Connection sinkConn, String rsId, long ssn, long scn, long t, String sqlRedo) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			sql = "insert into " + config.sinkTableSupplLogSync
					+ " (RS_ID,SSN,SCN,REMARK,INSERT_TIME) values (?,?,?,?,?)";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setString(1, rsId);
			pstmt.setLong(2, ssn);
			pstmt.setLong(3, scn);
			pstmt.setString(4, sqlRedo);
			pstmt.setLong(5,t);
			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}

}