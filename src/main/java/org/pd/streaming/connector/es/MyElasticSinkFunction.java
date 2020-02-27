package org.pd.streaming.connector.es;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings( "serial" )
public class MyElasticSinkFunction<T> implements ElasticsearchSinkFunction<T>
{
    String es_index;
    String es_type;
    
    final Logger logger = LoggerFactory.getLogger(MyElasticSinkFunction.class);
    ObjectMapper mapper = new ObjectMapper();           
    
    public MyElasticSinkFunction(String index, String type)
    {
        this.es_index = index;
        this.es_type = type;
    }
    
    public IndexRequest createIndexRequest(T element) throws JsonProcessingException 
    {
        String jsonData = mapper.writeValueAsString( element );
        
        // type is optional in ES 7.0+ but required in current dependency flink-connector-elasticsearch6_2.11.
        // May be when flink releases flink-connector-elasticsearch7_x.xx it will go away 
        return Requests.indexRequest()
                .index(es_index)
                .type( es_type )            
                .source( jsonData.getBytes(), XContentType.JSON );
    }

    @Override
    public void process(T element, RuntimeContext ctx, RequestIndexer indexer) 
    {
        logger.info( "ES Sink process called for element {}", element );
        
        try
        {
            indexer.add(createIndexRequest(element));
        }
        catch( JsonProcessingException e )
        {
            e.printStackTrace();
        }
    }
}