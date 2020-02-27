package org.pd.streaming.connector.es;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;

/**
 * Example of using Flink's Elasticsearch connector to store data.
 * 
 * @author preetdeep.kumar
 */
public class FlinkElasticSearchDemo 
{
	static List<HttpHost> esHost = Arrays.asList( new HttpHost( "127.0.0.1", 9200, "http" ) );
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception 
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// add a custom source which generates a random number between 0 and 100 every second
		DataStreamSource<SampleMessage> stream = env.addSource(new MessageGenerator());
		
		// Configure Elasticsearch as sink
        ElasticsearchSink.Builder<SampleMessage> esSinkBuilderMetric = 
            new ElasticsearchSink.Builder<>(esHost, new MyElasticSinkFunction<SampleMessage>("sample_message", "data") );
        
        esSinkBuilderMetric.setBulkFlushMaxActions(1);
        
        stream        
        .filter(new FilterFunction<SampleMessage>()
        {
        	@Override
			public boolean filter(SampleMessage message) throws Exception 
        	{
				// only retain those having value >= 70
        		return message.getMetric_value() >= 70;
			}
		})
        .keyBy((KeySelector<SampleMessage, String>) SampleMessage::getMetric_name)
        .timeWindow(Time.minutes(1))
        .apply(new WindowFunction<SampleMessage, SampleMessage, String, TimeWindow>() 
        {
			// sample business logic to compute count per metric_name based on the 
        	// value collected during 1 minute period
        	@Override
			public void apply(String metric_name, TimeWindow window, Iterable<SampleMessage> values, Collector<SampleMessage> out) throws Exception 
			{
				long minor_usage = 0, major_usage = 0, critical_usage = 0;
				
				for(SampleMessage msg : values)
				{
					if(msg.getMetric_value() < 80)
					{
						minor_usage++;	
					}
					else if(msg.getMetric_value() < 90)
					{
						major_usage++;
					}
					else
					{
						critical_usage++;
					}
				}
				
				out.collect(new SampleMessage(metric_name+"_usage_minor", minor_usage));
				out.collect(new SampleMessage(metric_name+"_usage_major", major_usage));
				out.collect(new SampleMessage(metric_name+"_usage_critical", critical_usage));
			}
		})
        .addSink(esSinkBuilderMetric.build());
        
        env.execute("flink-es-demo");
	}
}
