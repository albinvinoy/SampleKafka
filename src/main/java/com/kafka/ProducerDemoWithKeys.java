package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {

			String key = "id_" + i;

			ProducerRecord<String, String> producerRecord =
					new ProducerRecord<String, String>("first_topic", key, "hello world " + i);

			// send data - async
			producer.send(producerRecord, (recordMetadata, e) -> {
				// executes every time a record is successfully send or exception thrown
				if (e == null) {
					// record send
					System.out.print("Received new metadata. \n" +
							"TOPIC : " + recordMetadata.topic() + "\n" +
							"PARTITION " + recordMetadata.partition() + "\n" +
							"OFFSET " + recordMetadata.offset() + "\n");
				} else {
					logger.error("Error " + e);
				}
			});
		}

		//flush and close
		// wait for data to be send
		producer.flush();
		producer.close();
	}
}
