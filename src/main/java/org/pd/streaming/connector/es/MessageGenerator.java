package org.pd.streaming.connector.es;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings( "serial" )
public class MessageGenerator implements SourceFunction<SampleMessage>
{
    @Override
    public void run( SourceContext<SampleMessage> ctx ) throws Exception
    {
        while(true)
        {
        	ctx.collect( new SampleMessage("disk_usage", ThreadLocalRandom.current().nextLong(100)));
            ctx.collect( new SampleMessage("cpu_usage", ThreadLocalRandom.current().nextLong(100)));
            ctx.collect( new SampleMessage("memory_usage", ThreadLocalRandom.current().nextLong(100)));
            
            Thread.sleep( 1000 );
        }
    }

    @Override
    public void cancel()
    {
    }
}
