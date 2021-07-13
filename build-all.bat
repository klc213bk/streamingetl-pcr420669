@echo off 

set LOGMINER_HOME=C:\Users\ASDTEMP32\git\kafka-connect-oracle
set COMMON_HOME=C:\Users\ASDTEMP32\git\streamingetl-common
set APP_HOME=C:\Users\ASDTEMP32\git\streamingetl-pcr420669
set STREAMINGETL_HOME=C:\Users\ASDTEMP32\git\streamingetl

echo "start to build logminer"
cd %LOGMINER_HOME%
call mvn clean package
call xcopy %LOGMINER_HOME%\target\*.jar %STREAMINGETL_HOME%\connectors\oracle-logminer-connector /Y

echo "start to build app"
cd %APP_HOME%
call mvn clean package
call xcopy %APP_HOME%\pcr420669-consumer\target\*.jar %APP_HOME%\lib /Y
call xcopy %APP_HOME%\pcr420669-load\target\*.jar %APP_HOME%\lib /Y
call xcopy %APP_HOME%\pcr420669-test\target\*.jar %APP_HOME%\lib /Y
