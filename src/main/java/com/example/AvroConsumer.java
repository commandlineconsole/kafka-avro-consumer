package com.example;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

public class AvroConsumer {
	private final ConsumerConnector consumer;
	private final String topic;

	public AvroConsumer(String zookeeper, String groupId, String topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);

	}

	public void testConsumer(String path) {

		Map<String, Integer> topicMap = new HashMap<String, Integer>();

		// Define single thread for topic
		topicMap.put(topic, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);

		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);

		System.out.println("Inside testConsumer..waiting for incoming message ");

		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			while (consumerIte.hasNext()) {
				// System.out.println("Message from Single Topic :: "
				// + new String(consumerIte.next().message()));

				try {

					byte[] received_message = consumerIte.next().message();
					System.out.println("Received byte message: " + received_message);
					Schema schema = null;
					// schema = new Schema.Parser().parse(new
					// File("/Users/ypant/workspace/avro/book.avsc"));

					// schema = new Schema.Parser().parse(new
					// File("book.avsc"));
					schema = new Schema.Parser().parse(new File(path));

					DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);

					Decoder decoder = DecoderFactory.get().binaryDecoder(received_message, null);

					GenericRecord payload = null;
					payload = reader.read(null, decoder);
					System.out.println("Decoded message : " + payload);
				} catch (IOException ex) {
					System.out.println(ex.toString());
					System.out.println("File not found  ");
				}
			}
		}
		if (consumer != null)
			consumer.shutdown();
	}

	public static void main(String[] args) {

		String zooKeeper = args[0];
		String groupId = args[1];
		String topic = args[2];

		AvroConsumer consumer = new AvroConsumer(zooKeeper, groupId, topic);

		ClassLoader loader = AvroConsumer.class.getClassLoader();
		File file = new File(loader.getResource("schema/book.avsc").getFile());
		String path = file.getAbsolutePath();
		consumer.testConsumer(path);
	}
}
