# streamingetl-pcr420669

# add table-level supplemental log
SQL> ALTER TABLE PMUSER.TEST_T_ADDRESS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
SQL> ALTER TABLE PMUSER.TEST_T_CONTRACT_BENE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
SQL> ALTER TABLE PMUSER.TEST_T_INSURED_LIST ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
SQL> ALTER TABLE PMUSER.TEST_T_POLICY_HOLDER ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;



8:09.417 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>  Start: createTable, tableName=PUBLIC.T_PARTY_CONTACT, createTableFile=env-uat/createtable-T_PARTY_CONTACT.sql
17:38:09.969 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>  End: createTable DONE!!!
17:38:09.969 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - init tables span=1501, 
17:38:11.224 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>> max address id=8936276020
17:57:37.578 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - run load data span=1169110, 
17:57:37.579 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>  Start: createIndexes
18:12:54.686 [pool-17-thread-2] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>>> create index span=917080
18:24:36.375 [pool-17-thread-1] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>>> create index span=1618791
18:33:20.805 [pool-17-thread-3] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>>> create index span=2143198
18:33:20.811 [pool-17-thread-4] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>>> create index span=2143204
18:33:20.815 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - runCreateIndexes span=2143236, 
18:33:20.816 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>  Start: setContractBeneEmailNull
19:11:33.883 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - >>>  End: setContractBeneEmailNull DONE!!!, span=2293067
19:11:33.920 [main] INFO  c.t.s.pcr420669.load.InitialLoadApp - total load span=5605452, 