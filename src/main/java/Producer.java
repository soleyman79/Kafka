import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import proto.Message;

import java.util.Properties;

public class Producer {
    private final String topic;
    private final String message;
    KafkaProducer<String, byte[]> producer;

    public Producer(String topic, String message) {
        this.topic = topic;
        this.message = message;

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9094");
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "my-id");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(prop);
    }

    public void produce() {
        try {
            byte[] message = Message.newBuilder().setMessage(this.message).build().toByteArray();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, message);
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
