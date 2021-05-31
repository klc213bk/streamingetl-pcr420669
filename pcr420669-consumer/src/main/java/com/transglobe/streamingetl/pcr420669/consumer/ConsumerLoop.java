package com.transglobe.streamingetl.pcr420669.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

public class ConsumerLoop implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(ConsumerLoop.class);

	private static final Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static final Integer INSURED_LIST_ROLE_TYPE = 2;
	private static final Integer CONTRACT_BENE_ROLE_TYPE = 3;

	private final KafkaConsumer<String, String> consumer;
	private final int id;

	private Config config;

	private BasicDataSource connPool;

	private String streamingEtlHealthCdcTableName;

	public ConsumerLoop(int id,
			String groupId,  
			Config config,
			BasicDataSource connPool) {
		this.id = id;
		this.config = config;
		this.connPool = connPool;
		Properties props = new Properties();
		props.put("bootstrap.servers", config.bootstrapServers);
		props.put("group.id", groupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);

		String[] arr = config.sourceTableStreamingEtlHealthCdc.split("\\.");
		streamingEtlHealthCdcTableName = arr[1];

	}

	@Override
	public void run() {

		try {
			consumer.subscribe(config.topicList);

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				if (records.count() > 0) {
					Connection conn = null;
					int tries = 0;

					while (connPool.isClosed()) {
						tries++;
						try {
							connPool.restart();

							logger.info("   >>> Connection Pool restart, try {} times", tries);

							Thread.sleep(30000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}

					}
					conn = connPool.getConnection();

			
					for (ConsumerRecord<String, String> record : records) {
						Map<String, Object> data = new HashMap<>();
						try {	
							conn.setAutoCommit(false);
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

							String fullTableName = payload.get("SEG_OWNER").asText() + "." + payload.get("TABLE_NAME").asText();
						//	logger.info("   >>>operation={}, fullTableName={}", operation, fullTableName);
							if (!"T_STREAMING_ETL_HEALTH_CDC".equals(payload.get("TABLE_NAME").asText())) {
								logger.info("   >>>payload={}", payload.toPrettyString());
							}
							if (StringUtils.equals(streamingEtlHealthCdcTableName, payload.get("TABLE_NAME").asText())) {
								doHealth(conn, objectMapper, payload);
							} else if ("INSERT".equals(operation)) {
								//						logger.info("   >>>doInsert");
								doInsert(conn, objectMapper, payload);
								//						logger.info("   >>>doInsert DONE!!!");
							} else if ("UPDATE".equals(operation)) {
								//						logger.info("   >>>doUpdate");
								doUpdate(conn, objectMapper, payload);
								//						logger.info("   >>>doUpdate DONE!!!");
							} 
							conn.commit();
						} catch(Exception e) {
							logger.error(">>>message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e), data);
						}
					}
					
					conn.close();
				}


			}
		} catch (Exception e) {
			// ignore for shutdown 
			logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			consumer.close();

			if (connPool != null) {
				try {
					connPool.close();
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
	private void doInsert(Connection conn, ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String fullTableName = payload.get("SEG_OWNER").asText() + "." + payload.get("TABLE_NAME").asText();

		String data = payload.get("data").toString();

		if (config.sourceTablePolicyHolder.equals(fullTableName)
				|| config.sourceTableInsuredList.equals(fullTableName)
				|| config.sourceTableContractBene.equals(fullTableName)) {
			//			logger.info("   >>>insert partyContact");
			PartyContact partyContact = objectMapper.readValue(data, PartyContact.class);
			//			logger.info(">>> partyContact={}", partyContact);

			if (config.sourceTablePolicyHolder.equals(fullTableName)) {
				partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
			} else if (config.sourceTableInsuredList.equals(fullTableName)) {
				partyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
			} else if (config.sourceTableContractBene.equals(fullTableName)) {
				partyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
				partyContact.setEmail(null); // 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
			}
			//			logger.info(">>> start insertPartyContact");
			insertPartyContact(conn, partyContact);

		} else if (config.sourceTableAddress.equals(fullTableName)) {
			Address address = objectMapper.readValue(data, Address.class);
			//			logger.info(">>> address={}", address);
			//			logger.info("   >>>insert Address");
			insertAddress(conn, address);
		}


	}

	private void insertPartyContact(Connection conn, PartyContact partyContact) throws Exception  {
		PreparedStatement pstmt = null;

		String sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact 
				+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
		int count = getCount(conn, sql);
		if (count == 0) {
			if (partyContact.getAddressId() == null) {
				sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL) " 
						+ " values (?,?,?,?,?,?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, partyContact.getRoleType());
				pstmt.setLong(2, partyContact.getListId());
				pstmt.setLong(3, partyContact.getPolicyId());
				pstmt.setString(4, partyContact.getName());
				pstmt.setString(5, partyContact.getCertiCode());
				pstmt.setString(6, partyContact.getMobileTel());
				pstmt.setString(7, partyContact.getEmail());

				pstmt.executeUpdate();
				pstmt.close();
			} else {
				String address1 = null;
				address1 = getAddress1FromPartyContactTemp(conn, partyContact.getAddressId());
				if (StringUtils.isBlank(address1)) {
					address1 = getAddress1FromPartyContact(conn, partyContact.getAddressId());
				} else {
					// delete PartyContactTemp
					deletePartyContactTemp(conn, partyContact.getAddressId());
				}
				sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
						+ " values (?,?,?,?,?,?,?,?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, partyContact.getRoleType());
				pstmt.setLong(2, partyContact.getListId());
				pstmt.setLong(3, partyContact.getPolicyId());
				pstmt.setString(4, partyContact.getName());
				pstmt.setString(5, partyContact.getCertiCode());
				pstmt.setString(6, partyContact.getMobileTel());
				pstmt.setString(7, partyContact.getEmail());
				pstmt.setLong(8, partyContact.getAddressId());
				pstmt.setString(9, address1);
				
				pstmt.executeUpdate();
				pstmt.close();
			}
			
		} else {
			// record exists, error
			String error = String.format("table=%s record already exists, role_type=%d, list_id=%d", config.sinkTablePartyContact, partyContact.getRoleType(), partyContact.getListId());
			throw new Exception(error);
		}
	}

	private void insertAddress(Connection conn, Address address) throws Exception {

		String sql = null;
		PreparedStatement pstmt = null;
		try {
			sql = "select ROLE_TYPE,LIST_ID from " + config.sinkTablePartyContact + " where address_id = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());
			ResultSet resultSet = pstmt.executeQuery();
			int count = 0;
			while (resultSet.next()) {
				count++;
				break;

			}
			resultSet.close();
			pstmt.close();

			if (count == 0) {
				// insert 
				sql = "insert into " + config.sinkTablePartyContactTemp + " (ADDRESS_ID,ADDRESS_1)" + " values (?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setLong(1, address.getAddressId());
				pstmt.setString(2, address.getAddress1());
				pstmt.executeUpdate();
				pstmt.close();

			} else {
				// update party contact
				sql = "update "  + config.sinkTablePartyContact + " set address_1 = ? where address_id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setString(1, address.getAddress1());
				pstmt.setLong(2, address.getAddressId());
				pstmt.executeUpdate();
				pstmt.close();
			}

		} finally {
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
	private String getAddress1FromPartyContact(Connection conn, Long addressId) throws SQLException {

		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		String address1 = null;
		try {
			String sql = "select ADDRESS_1 from " + config.sinkTablePartyContact + " where address_id = " + addressId;
			pstmt = conn.prepareStatement(sql);
			resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				address1 = resultSet.getString("ADDRESS_1");
				break;
			}
			resultSet.close();
			pstmt.close();

		} finally {
			if (resultSet != null) resultSet.close();
			if (pstmt != null) pstmt.close();
		}

		return address1;
	}

	private String getAddress1FromPartyContactTemp(Connection conn, Long addressId) throws SQLException {

		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		String address1 = null;
		try {
			String sql = "select ADDRESS_1 from " + config.sinkTablePartyContactTemp + " where address_id = " + addressId;

			pstmt = conn.prepareStatement(sql);
			resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				address1 = resultSet.getString("ADDRESS_1");
				break;
			}

			resultSet.close();
			pstmt.close();

		} finally {
			if (resultSet != null) resultSet.close();
			if (pstmt != null) pstmt.close();
		}

		return address1;
	}

	private void deletePartyContactTemp(Connection conn, Long addressId) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "delete from " + config.sinkTablePartyContactTemp + " where address_id = " + addressId;
			pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();

			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private void doUpdate(Connection conn, ObjectMapper objectMapper, JsonNode payload) throws Exception {
		String fullTableName = payload.get("SEG_OWNER").asText() + "." + payload.get("TABLE_NAME").asText();
		//		logger.info(">>> fulltableName={}", fullTableName);
		String data = payload.get("data").toString();
		String before = payload.get("before").toString();

		if (config.sourceTablePolicyHolder.equals(fullTableName)
				|| config.sourceTableInsuredList.equals(fullTableName)
				|| config.sourceTableContractBene.equals(fullTableName)) {
			PartyContact oldpartyContact = objectMapper.readValue(before, PartyContact.class);
			PartyContact newpartyContact = objectMapper.readValue(data, PartyContact.class);
			//			logger.info(">>> oldpartyContact={}", oldpartyContact);
			//			logger.info(">>> newpartyContact={}", newpartyContact);

			if (config.sourceTablePolicyHolder.equals(fullTableName)) {
				oldpartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
				newpartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
			} else if (config.sourceTableInsuredList.equals(fullTableName)) {
				oldpartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
				newpartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
			} else if (config.sourceTableContractBene.equals(fullTableName)) {
				oldpartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
				newpartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
				newpartyContact.setEmail(null); // 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
			}

			updatePartyContact(conn, oldpartyContact, newpartyContact);

		} else if (config.sourceTableAddress.equals(fullTableName)) {
			Address oldAddress = objectMapper.readValue(before, Address.class);
			Address newAddress = objectMapper.readValue(data, Address.class);
			//			logger.info(">>> oldAddress={}", oldAddress);
			//			logger.info(">>> newAddress={}", newAddress);

			updateAddress(conn, oldAddress, newAddress);
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
			pstmt.setString(1, newAddress.getAddress1());
			pstmt.setLong(2, oldAddress.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

			// update PartyContactTemp
			sql = "update " + config.sinkTablePartyContactTemp
					+ " set ADDRESS_1 = ? where ADDRESS_ID = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, newAddress.getAddress1());
			pstmt.setLong(2, oldAddress.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private void updatePartyContact(Connection conn, PartyContact oldPartyContact, PartyContact newPartyContact) throws Exception  {
		String sql = "";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			// 0: no party address update, 1: add party address, 2: remove party address, 3: update party address
			int addressAction = 0;  
			if (oldPartyContact.getAddressId() == null && newPartyContact.getAddressId() == null) {
				addressAction = 0;
			} else if (oldPartyContact.getAddressId() == null && newPartyContact.getAddressId() != null) {
				addressAction  = 1;
			} else if (oldPartyContact.getAddressId() != null && newPartyContact.getAddressId() == null) {
				addressAction  = 2;
			} else if (oldPartyContact.getAddressId() != null && newPartyContact.getAddressId() != null) {
				if (oldPartyContact.getAddressId().intValue() == newPartyContact.getAddressId().intValue()) {
					addressAction = 0;
				} else {
					addressAction = 3;
				}
			} else {
				throw new Exception("Error: impossibly goes here!!!");
			}
			logger.info(">>> addressAction={}, old addressid={}, new adressid={}", addressAction, oldPartyContact.getAddressId(), newPartyContact.getAddressId());
			logger.info(">>> oldPartyContact.ListId={}, newPartyContact.ListId={}", oldPartyContact.getListId(), newPartyContact.getListId());
			Long listId = null;
			if (oldPartyContact.getListId() == null && newPartyContact.getListId() == null) {
				throw new Exception("Error: old ListId=null, new ListId=null");
			} else if (oldPartyContact.getListId() == null && newPartyContact.getListId() != null) {
				throw new Exception("Error: old ListId=null, new ListId=" + newPartyContact.getListId());
			} else if (oldPartyContact.getListId() != null && newPartyContact.getListId() == null) {
				throw new Exception("Error: old ListId=" + oldPartyContact.getListId() + ", new ListId=null");
			} else if (oldPartyContact.getListId() != null && newPartyContact.getListId() != null) {
				if (oldPartyContact.getListId().longValue() != newPartyContact.getListId().longValue()) {
					throw new Exception("Error: Not equal, old ListId=" + oldPartyContact.getListId() + ", new ListId=" + newPartyContact.getListId());
				}
				listId = oldPartyContact.getListId();
			}
			// 0: no party address update, 1: add party address, 2: remove party address, 3: update party address
			if (addressAction == 0) {
				// no party address update
				sql = "update " + config.sinkTablePartyContact 
						+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?" 
						+ " where ROLE_TYPE = ? and LIST_ID = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setLong(1, newPartyContact.getPolicyId());
				pstmt.setString(2, newPartyContact.getName());
				pstmt.setString(3, newPartyContact.getCertiCode());
				pstmt.setString(4, newPartyContact.getMobileTel());
				pstmt.setString(5, newPartyContact.getEmail());
				pstmt.setInt(6, oldPartyContact.getRoleType());
				pstmt.setLong(7, listId);

				pstmt.executeUpdate();
				pstmt.close();
			} else if (addressAction == 1 || addressAction == 3 ) {
				// add party address with address_id
				// 1. get address 1 
				//   check if existing address exists
				//   1.1 check party_contact_temp with address_id
				//      1.1.1 if exists, 
				//            { a. get address_1;
				//              b. delete from party_contact_temp } 
				//      1.1.2 if does not exist,
				//            1.1.2.1 check party_contact with address_id
				//                    1.1.2.1.1 if exists, 
				//                      			{ a. get address_1;}
				//                    1.1.2.1.2 if does not exist,
				//              					{ a. address_1 = null; }
				//  2. update party_contact with address_1          

				String address1 = null;

				sql = "select address_1 from " + config.sinkTablePartyContactTemp 
						+ " where address_id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setLong(1, newPartyContact.getAddressId());
				rs = pstmt.executeQuery();

				while (rs.next()) {
					address1 = rs.getString("ADDRESS_1");
				}
				rs.close();
				pstmt.close();
				
				logger.info(">>> PartyContactTemp address1 = {}", address1);
				
				if (address1 != null) {
					// delete from party_contact_temp 
					deletePartyContactTemp(conn, newPartyContact.getAddressId());

					logger.info(">>> delete PartyContactTemp sql={}, address_id= {}", sql, newPartyContact.getAddressId());
				} else {
					// check party_contact with address_id
					sql = "select address_1 from " + config.sinkTablePartyContact
							+ " where address_id = ?";
					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, newPartyContact.getAddressId());
					rs = pstmt.executeQuery();

					while (rs.next()) {
						address1 = rs.getString("ADDRESS_1");
					}
					rs.close();
					pstmt.close();
				}
				//update party_contact with address_1 
				sql = "update " + config.sinkTablePartyContact 
						+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=? " 
						+ " where ROLE_TYPE = ? and LIST_ID = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setLong(1, newPartyContact.getPolicyId());
				pstmt.setString(2, newPartyContact.getName());
				pstmt.setString(3, newPartyContact.getCertiCode());
				pstmt.setString(4, newPartyContact.getMobileTel());
				pstmt.setString(5, newPartyContact.getEmail());
				pstmt.setLong(6, newPartyContact.getAddressId());
				pstmt.setString(7, address1);
				pstmt.setInt(8, oldPartyContact.getRoleType());
				pstmt.setLong(9, listId);

				pstmt.executeUpdate();
				pstmt.close();
			} else if (addressAction == 2) {
				// remove party address 
				sql = "update " + config.sinkTablePartyContact 
						+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=? " 
						+ " where ROLE_TYPE = ? and LIST_ID = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setLong(1, newPartyContact.getPolicyId());
				pstmt.setString(2, newPartyContact.getName());
				pstmt.setString(3, newPartyContact.getCertiCode());
				pstmt.setString(4, newPartyContact.getMobileTel());
				pstmt.setString(5, newPartyContact.getEmail());
				pstmt.setLong(6, newPartyContact.getAddressId());
				pstmt.setString(7, null);
				pstmt.setInt(8, oldPartyContact.getRoleType());
				pstmt.setLong(9, listId);

				pstmt.executeUpdate();
				pstmt.close();
			} 

		} finally {
			if (pstmt != null) pstmt.close();
		}

	}

}