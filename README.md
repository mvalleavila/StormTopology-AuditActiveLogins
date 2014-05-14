# Storm-0.9.1-Kafka-0.8-Test

## Description

Basic example to test Storm-Kafka-0.8-plus:
  https://github.com/wurstmeister/storm-kafka-0.8-plus
  
## Compilation
  mvn clean package
  
## Run
  
  -> NOTE: Edit storm conf file cluster.xml for change WARN log level to INFO
  
  Submit the topology to Storm cluster:
  
  storm jar target/storm-kafka-0.8-plus-test-0.1.0-SNAPSHOT.jar storm.kafka.KafkaSpoutTestTopology <zookeepers host:port> <kafka topic-name> <storm topology-name> <storm-nimbus host> <storm-nimbus port>
  
  -> Example:
  
  storm jar target/storm-kafka-0.8-plus-test-0.1.0-SNAPSHOT.jar storm.kafka.KafkaSpoutTestTopology hadoop-manager:2181,hadoop-node1:2181,hadoop-node2:2181 test-topic test-topology streaming1 6627
  
  To see the output open workers logs, Kafka messages will be print in these logs.
  

