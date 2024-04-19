import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class Producer {
    public static void main(String[] arg) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        prop.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 30000);
        prop.put(ProducerConfig.TRANSACTION_TIMEOUT_DOC, 30000);
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 10000);
        prop.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prop)) {
            String topic = "a";
            String message = "Hello, Kafka!";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            List<PartitionInfo> result = producer.partitionsFor(topic);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("Message sent successfully to topic: " + metadata.topic());
                    } else {
                        System.err.println("Error sending message: " + exception.getMessage());
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
