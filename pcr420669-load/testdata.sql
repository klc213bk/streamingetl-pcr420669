
insert into test_T_POLICY_HOLDER
(
select a.*
from T_POLICY_HOLDER  a inner join T_CONTRACT_MASTER b ON a.POLICY_ID=b.POLICY_ID 
 left join T_ADDRESS c on a.address_id = c.address_id 
 where b.LIABILITY_STATE = 0 and 0 <= a.list_id and a.list_id < 10000
 );
 
 insert into test_T_POLICY_HOLDER_LOG
select a.*
from T_POLICY_HOLDER_LOG a left join T_ADDRESS c on a.address_id = c.address_id 
 where a.LAST_CMT_FLG = 'Y' and 0 <= a.log_id and a.log_id < 75140000; 
 
 ---- insured list
insert into test_T_INSURED_LIST
select a.*
from T_INSURED_LIST  a inner join T_CONTRACT_MASTER b ON a.POLICY_ID=b.POLICY_ID 
 left join T_ADDRESS c on a.address_id = c.address_id 
 where b.LIABILITY_STATE = 0 and 12990000 <= a.list_id and a.list_id < 13000000; 
 
 insert into test_T_INSURED_LIST_LOG
select a.*
from T_INSURED_LIST_LOG a left join T_ADDRESS c on a.address_id = c.address_id 
 where a.LAST_CMT_FLG = 'Y' and 83900000 <= a.log_id and a.log_id < 83920000; 
 
 
  ---- contract bene
 insert into test_T_CONTRACT_BENE
select a.*
from T_CONTRACT_BENE  a inner join T_CONTRACT_MASTER b ON a.POLICY_ID=b.POLICY_ID 
 left join T_ADDRESS c on a.address_id = c.address_id 
 where b.LIABILITY_STATE = 0 and 19800000 <= a.list_id and a.list_id < 19830000; 
 
 insert into test_T_CONTRACT_BENE_LOG
select a.*
from T_CONTRACT_BENE_LOG a left join T_ADDRESS c on a.address_id = c.address_id 
 where a.LAST_CMT_FLG = 'Y' and 339500000 <= a.log_id and a.log_id < 339550000; 
 