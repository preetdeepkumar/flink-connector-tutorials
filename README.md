# Apache Flink Connector Examples
Examples of Flink's in-built connectors with various external systems such as Kafka, Elasticsearch, S3 etc. I will also share few custom connectors using Flink's RichSourceFunction API. My blogs on [dzone.com](dzone.com) refers to these examples. This project will be updated with new examples. For official Flink documentation please visit [https://flink.apache.org/](https://flink.apache.org/)

### Package - org.pd.streaming.connector.kafka
* FlinkKafkaDemo is the main class here which uses Flink's kafka connector to read apache log data from kafka.
* It also uses Flink's Elasticsearch connector to store data after computation.
* The data send to kafka needs to be configured using Logstash reading Apache Server access log files. The config file
I used for testing is copied to src/main/resources/logstash-apache-config.yml.
* Mapping for 'bad-client' index needs to be created in the Elasticsearch. Please see src/main/resources/es-mapping.txt
* Mapping for 'metric' index will be automatically created by Elasticsearch.
* This class showcases a simple but effective log analytics solution by creating two pipelines from single Flink stream.
	* First pipeline will define a time window and compute 4xx and 5xx response codes in each log records.
	* Second pipeline will filter out unwanted log record and match the clientIP against a blacklisted IP.
	* Both these pipelines will send the result to Elasticsearch.
	
* Flink Kafka doc 
	- https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html#apache-kafka-connector

### Package - org.pd.streaming.connector.kafka.examples
* It contains 3 simple examples using FlinkKafkaConsumer and FlinkKafkaProducer API
* NumberGenerator and Producer classes are used to generate numbers every second and send to Kafka Topics
* KafkaRecord is a wrapper to read both key and value
* MySchema class implements KafkaDeserializationSchema<KafkaRecord> and creates objects of type KafkaRecord
* Main1 - Read from Kafka, Define a 5 seconds Tumbling window, Simple reduction and print result to console
* Main2 - Read from Kafka, Define a 5 seconds Tumbling window, Simple reduction and send result to Kafka Topic
* Main3 - Read from Kafka, Filter bad values, Define a 5 seconds Tumbling window with allowedLateness, Simple aggregation and send result to Kafka Topic
	
* Kafka topics required to run above example
	- kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Topic1-IN
	- kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Topic2-IN
	- kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Topic3-IN
	- kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Topic2-OUT
	- kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Topic3-OUT

### Package - org.pd.streaming.connector.es
* FlinkElasticSearchDemo is the main class here which uses Flink's Elasticsearch connector to send data to an Elasticsearch
index.
* It uses a MessageGenerator class as source which is a simple name/value generator POJO.
* The stream is keyed by the 'metric_name' coming in the SampleMessage object and some computation is done before it is stored in Elasticsearch.

* Flink ES doc 
	- https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/elasticsearch.html#elasticsearch-connector

## Prerequisites
* Java 8
* lombok (https://projectlombok.org/)
