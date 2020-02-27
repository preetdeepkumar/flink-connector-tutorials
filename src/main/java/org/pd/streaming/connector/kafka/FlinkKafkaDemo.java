package org.pd.streaming.connector.kafka;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.pd.streaming.connector.es.MyElasticSinkFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

/**
 * This example demonstrate a log analytics use case where a logstash is externally 
 * configured to send apache http server logs to kafka. This Flink code is ingesting data
 * from Kafka, transforming it to a Java POJO, doing some computation and storing in Elasticsearch. 
 * 
 * This example assumes both Elasticsearch and Kafka Broker is available on localhost. Please change the 
 * IP as per your configuration. 
 * 
 * This example also shows how to use EventTime and assign timestamp overriding default ProcessTime.
 * 
 * @author preetdeep.kumar
 */
public class FlinkKafkaDemo
{
    static final ObjectMapper mapper = new ObjectMapper();
    
    static final String BLACKLIST_IP_PREFIX = "12.*" ;

    static final AtomicLong totalRequests = new AtomicLong();
    
    static final SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    
    static List<HttpHost> esHost = Arrays.asList( new HttpHost( "127.0.0.1", 9200, "http" ) );
    
    @SuppressWarnings( { "serial" } )
    public static void main( String[] args ) throws Exception
    {
        // Initialize Flink Stream Execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // To use timestamp available in the data instead of using system time clock
        env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime );

        ////////////////////////////////////////
        // Configure Elasticsearch sink for "metric" index
        ElasticsearchSink.Builder<Metric> esSinkBuilderMetric = 
            new ElasticsearchSink.Builder<>(esHost, new MyElasticSinkFunction<Metric>("metric", "data") );
        
        esSinkBuilderMetric.setBulkFlushMaxActions(1);
        
        // Configure Elasticsearch sink for "payload" index
        ElasticsearchSink.Builder<Payload> esSinkBuilderPayload = 
            new ElasticsearchSink.Builder<>(esHost, new MyElasticSinkFunction<Payload>("bad-client", "_doc") );
        
        esSinkBuilderPayload.setBulkFlushMaxActions(1);
        ////////////////////////////////////////
        
        
        ////////////////////////////////////////
        // Configure Kafka Client //////////////
        String topic = "flink-log-test";
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        ////////////////////////////////////////
        
        
        // create a stream to ingest data from Kafka as a string
        DataStream<String> stream = env.addSource(kafkaConsumer);
        
        // transform the data to Java POJO and add timestamp
        SingleOutputStreamOperator<ApacheLogKafka> apacheLogStream = 
            stream
            .map( new MapFunction<String,ApacheLogKafka>()
            {
                // Serialize to ApacheLogKafka class from JSON String
                @Override
                public ApacheLogKafka map( String value ) throws Exception
                {
                    return mapper.readValue( value, ApacheLogKafka.class );
                }
            })
            .assignTimestampsAndWatermarks( new AscendingTimestampExtractor<ApacheLogKafka>()
            {
                // assign timestamp and watermarks using timestamp from stream
                @Override
                public long extractAscendingTimestamp( ApacheLogKafka message )
                {
                    try
                    {
                        return df.parse( message.getTimestamp() ).getTime();
                    }
                    catch( ParseException e )
                    {
                        return System.currentTimeMillis();
                    } 
                }
            });
        
        ////////////////////////////////////////
        // first pipeline for security anomalies
        ////////////////////////////////////////
        
        apacheLogStream            
        .timeWindowAll( Time.seconds( 30 ) )
        .apply( new AllWindowFunction<ApacheLogKafka, Metric, TimeWindow>()
        {
            @Override
            public void apply( TimeWindow window, Iterable<ApacheLogKafka> values, Collector<Metric> out ) throws Exception
            {
                long currentTotal = totalRequests.addAndGet(((Collection<?>) values).size());
                
                long clientErrCount = 0;
                long serverErrCount = 0;
                
                for( ApacheLogKafka msg : values) 
                { 
                    if ( msg.getResponse().startsWith( "4" ) ) 
                    { 
                        clientErrCount++;
                    } 
                    else if ( msg.getResponse().startsWith( "5" ) ) 
                    { 
                        serverErrCount++;
                    }
                }
                          
                String computedTime = LocalDateTime.now(ZoneId.of( "Z" )).toString();
                
                out.collect( new Metric("client_errors", clientErrCount, computedTime) );
                out.collect( new Metric("server_errors", serverErrCount, computedTime) );
                out.collect( new Metric("total_requests", currentTotal, computedTime) );
            }
        })
        .addSink( esSinkBuilderMetric.build() );
        
        /////////////////////////////////////
        // second pipeline for rogue clients
        /////////////////////////////////////
        
        apacheLogStream
        .filter( new FilterFunction<ApacheLogKafka>()
        {
            // only retain log messages having error code 4xx
        	@Override
            public boolean filter( ApacheLogKafka log ) throws Exception
            {
                return log.getResponse().startsWith( "4" );
            }
        })
        .process( new ProcessFunction<ApacheLogKafka, Payload>()
        {
            @Override
            public void processElement( ApacheLogKafka log,
                                        ProcessFunction<ApacheLogKafka, Payload>.Context context,
                                        Collector<Payload> output )
                throws Exception
            {
                if(log.getClientip().matches(BLACKLIST_IP_PREFIX))
                {
                    // create payload for ES only if the clientIP is in blacklist
                	Payload payload = new Payload( log.getClientip(), log.getGeoip().getLocation(), log.getTimestamp() );
                    output.collect( payload);
                }
            }
        })        
        .addSink( esSinkBuilderPayload.build() );
        
        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );
        
        // start flink
        env.execute();
    }
    
    @Data
    static class Payload
    {
       String ip;
       Location location;
       String timestamp;
       
       public Payload( String clientip, Location location, String timestamp )
       {
           this.ip = clientip;
           this.location = location;
           this.timestamp = timestamp;
       }
    }
    
    @Data
    static class Metric
    {
        String metric_name;
        Long metric_value;
        String timestamp;
        
        public Metric(String metric_name, Long metric_value, String timestamp)
        {
            this.metric_name = metric_name;
            this.metric_value = metric_value;
            this.timestamp = timestamp;
        }
    }
}
