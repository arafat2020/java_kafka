package Producer;

import java.beans.IntrospectionException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKey {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            runner(i);
        }
    }

    private static void runner(int id) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerInIt.class);

        String topic = "Test_Topic";
        String value = "VAL_" + Integer.toString(id);
        String key = "ID_" + Integer.toString(id);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        logger.info(key + value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.info("❌ Error sending record: " + exception.getMessage());
            } else {
                logger.error("✅ Sent to topic \n" + metadata.topic() +
                        " partition \n" + metadata.partition() +
                        " offset \n" + metadata.offset());
            }
        }).get(); // it should never be done in productions

        producer.flush();
        producer.close();
    }
}
