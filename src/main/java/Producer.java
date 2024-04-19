import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] arg) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9094");
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prop)) {
            String topic = "topic-b";
            String message = "Hello, Kafka!";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

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
