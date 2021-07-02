@echo off 

set APP_HOME=C:\Users\ASDTEMP32\git\streamingetl-pcr420669

call java -cp %APP_HOME%\lib\pcr420669-consumer-1.0.jar;%APP_HOME%\lib\* -Dprofile.active=env-dev2 com.transglobe.streamingetl.pcr420669.consumer.ConsumerApp
