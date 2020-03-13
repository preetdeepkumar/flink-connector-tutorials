package org.pd.streaming.connector.kafka.examples;

import java.time.LocalTime;
import java.util.Properties;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main2
{
	static String TOPIC_IN = "Topic2-IN";
	static String TOPIC_OUT = "Topic2-OUT";
	static String BOOTSTRAP_SERVER = "localhost:9092";
	
    @SuppressWarnings("serial")
	public static void main( String[] args ) throws Exception
    {
    	Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());
    	
    	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example2");
        
        // Alternate consumer to get only values per Topic
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new SimpleStringSchema(), props);
        kafkaConsumer.setStartFromLatest();
        
        // Create Kafka producer from Flink API
		Properties prodProps = new Properties();
		prodProps.put("bootstrap.servers", BOOTSTRAP_SERVER);
		
		FlinkKafkaProducer<String> kafkaProducer = 
				new FlinkKafkaProducer<String>(TOPIC_OUT, 
											   ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_OUT, "myKey".getBytes(), value.getBytes())), 
											   prodProps, 
											   Semantic.EXACTLY_ONCE);
		
		// create a stream to ingest data from Kafka with value as String
        DataStream<String> stream = env.addSource(kafkaConsumer);
        
        stream        
        .timeWindowAll(Time.seconds(5)) // ignoring grouping per key
        .reduce(new ReduceFunction<String>() 
        {
        	@Override
			public String reduce(String value1, String value2) throws Exception 
			{
				System.out.println(LocalTime.now() + " -> " + value1 + "   " + value2);
				return value1+value2;
			}
		})
        .addSink(kafkaProducer);
        
		// produce a number as string every second
		new NumberGenerator(p, TOPIC_IN).start();
		
        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );
        
        // start flink
        env.execute();
    }
}
