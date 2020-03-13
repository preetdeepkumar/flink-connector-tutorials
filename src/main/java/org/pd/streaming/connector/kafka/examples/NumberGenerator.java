package org.pd.streaming.connector.kafka.examples;

public class NumberGenerator extends Thread 
{
	int counter = 0;
	final Producer<String> p;
	final String topic;

	public NumberGenerator(Producer<String> p, String topic)
	{
		this.p = p;
		this.topic = topic;
	}

	@Override
	public void run() 
	{
		try 
		{
			while( ++counter > 0 )
			{
				p.send(topic, "[" + counter + "]");

				Thread.sleep( 1000 );
			}
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
