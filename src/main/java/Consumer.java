import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import proto.Message;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private final String topic;
    private final KafkaConsumer<String, byte[]> consumer;

    public Consumer(String topic) {
        this.topic = topic;

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-id");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(prop);
    }

    public void consume() {
        consumer.subscribe(Arrays.asList(this.topic));
        boolean flg = true;
        while (flg) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, byte[]> record : records) {
                flg = false;
                try {
                    Message message = Message.parseFrom(record.value());
                    String value = message.getMessage();
                    System.out.println("Value: " + value + "\nTimestamp: " + record.timestamp());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
