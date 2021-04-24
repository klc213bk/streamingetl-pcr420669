
 select a.* from t_policy_holder a inner join t_address b on a.address_id = b.address_id 
 where b.address_id > 100000
 fetch next 10000 rows only;
 
 select b.* from t_policy_holder a inner join t_address b on a.address_id = b.address_id 
 where b.address_id > 100000
 fetch next 10000 rows only;

------------------
 select a.* from t_insured_list a inner join t_address b on a.address_id = b.address_id 
 where b.address_id > 100000
 fetch next 10000 rows only;
 
  select b.* from t_insured_list a inner join t_address b on a.address_id = b.address_id 
 where b.address_id > 100000
 fetch next 10000 rows only;
 
 ----------------------------------------
 
 select a.* from t_contract_bene a inner join t_address b on a.address_id = b.address_id 
 where b.address_id > 100000
 fetch next 10000 rows only;
 
  select b.* from t_contract_bene a inner join t_address b on a.address_id = b.address_id 
 where b.address_id > 100000
 fetch next 10000 rows only;
 
 