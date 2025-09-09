package Producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerInIt {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerInIt.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("Test_Topic", "hello world");
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
               logger.info("❌ Error sending record: " + exception.getMessage());
            } else {
               logger.error("✅ Sent to topic " + metadata.topic() +
                        " partition " + metadata.partition() +
                        " offset " + metadata.offset());
            }
        });

        producer.flush();
        producer.close();

    }
}
