###### streamingetl-pcr420669
 
1. create topics(Optional, first time)
	$./create-kafka-topics.sh
	
2. start ignite server
  	$./start-ignite.sh
  	
3. Initial load data (Optional only for reload data)
	$./start-initialload.sh  	
	
4. start kafka consumer
 	$ ./start-consumer.sh
 	
5. start logminer
	$ ./start-logminer.sh 	 	
  	