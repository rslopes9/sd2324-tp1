package tukano.impl.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordProcessor {
	void onReceive(ConsumerRecord<String, String> r);
}