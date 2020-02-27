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
