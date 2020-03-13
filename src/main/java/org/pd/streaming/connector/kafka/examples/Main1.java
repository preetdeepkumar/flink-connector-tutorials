package org.pd.streaming.connector.kafka.examples;

import java.time.LocalTime;
import java.util.Properties;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main1
{
	static String TOPIC_IN = "Topic1-IN";
	static String BOOTSTRAP_SERVER = "localhost:9092";
	
    @SuppressWarnings("serial")
	public static void main( String[] args ) throws Exception
    {
    	Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());
    	
    	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example1");
        
        // Reading data directly as <Key, Value> from Kafka. Write an inner class containing key, value 
        // and use it to deserialise Kafka record.
        // Reference => https://stackoverflow.com/questions/53324676/how-to-use-flinkkafkaconsumer-to-parse-key-separately-k-v-instead-of-t
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new MySchema(), props);
        
        kafkaConsumer.setStartFromLatest();
        
        // create a stream to ingest data from Kafka as a custom class with explicit key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);
        
        // supports timewindow without group by key
        stream
        .timeWindowAll(Time.seconds(5))
        .reduce(new ReduceFunction<KafkaRecord>() 
        {
        	KafkaRecord result = new KafkaRecord();
        	
        	@Override
			public KafkaRecord reduce(KafkaRecord record1, KafkaRecord record2) throws Exception 
			{
				System.out.println(LocalTime.now() + " -> " + record1 + "   " + record2);
				
				result.key = record1.key;  
				result.value = record1.value + record2.value;
				
				return result;
			}
		})
        .print(); // immediate printing to console
        
        //.keyBy( (KeySelector<KafkaRecord, String>) KafkaRecord::getKey )
        //.timeWindow(Time.seconds(5))
        
		// produce a number as string every second
		new NumberGenerator(p, TOPIC_IN).start();
		
        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );
        
        // start flink
        env.execute();
    }
}
