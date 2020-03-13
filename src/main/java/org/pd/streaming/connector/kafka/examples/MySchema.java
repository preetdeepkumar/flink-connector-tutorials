package org.pd.streaming.connector.kafka.examples;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@SuppressWarnings("serial")
public class MySchema implements KafkaDeserializationSchema<KafkaRecord>
{
	@Override
	public boolean isEndOfStream(KafkaRecord nextElement) {
		return false;
	}

	@Override
	public KafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		KafkaRecord data = new KafkaRecord();
		data.key =  new String(record.key());
		data.value = new String(record.value());
		data.timestamp = record.timestamp();
		
		return data;
	}

	@Override
	public TypeInformation<KafkaRecord> getProducedType() {
		return TypeInformation.of(KafkaRecord.class);
	}
}



