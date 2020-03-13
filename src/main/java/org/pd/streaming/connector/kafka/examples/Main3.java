package org.pd.streaming.connector.kafka.examples;

import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main3
{
	static String TOPIC_IN = "Topic3-IN";
	static String TOPIC_OUT = "Topic3-OUT";
	static String BOOTSTRAP_SERVER = "localhost:9092";
	
    @SuppressWarnings("serial")
	public static void main( String[] args ) throws Exception
    {
    	Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());
    	
    	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    	
    	// to use allowed lateness
    	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example3");
        
        // consumer to get both key/values per Topic
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new MySchema(), props);
        
        // for allowing Flink to handle late elements
        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<KafkaRecord>() 
        {
        	@Override
			public long extractAscendingTimestamp(KafkaRecord record) 
        	{
        		return record.timestamp;
			}
		});
        
        kafkaConsumer.setStartFromLatest();
        
        // Create Kafka producer from Flink API
		Properties prodProps = new Properties();
		prodProps.put("bootstrap.servers", BOOTSTRAP_SERVER);
		
		FlinkKafkaProducer<String> kafkaProducer = 
				new FlinkKafkaProducer<String>(TOPIC_OUT, 
											   ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_OUT, "myKey".getBytes(), value.getBytes())), 
											   prodProps, 
											   Semantic.EXACTLY_ONCE);
		
		// create a stream to ingest data from Kafka with key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);
        
        stream        
        .filter((record) -> record.value != null && !record.value.isEmpty())
        .keyBy(record -> record.key)
        .timeWindow(Time.seconds(5))
        .allowedLateness(Time.milliseconds(500))        
        .aggregate(new AggregateFunction<KafkaRecord, String, String>()  // kafka aggregate API is very simple but same can be achieved by Flink's reduce
        {
        	@Override
			public String createAccumulator() {
        		return "";
			}

			@Override
			public String add(KafkaRecord record, String accumulator) {
				return accumulator + record.value.length();
			}

			@Override
			public String getResult(String accumulator) {
				return accumulator;
			}

			@Override
			public String merge(String a, String b) {
				return a+b;
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
