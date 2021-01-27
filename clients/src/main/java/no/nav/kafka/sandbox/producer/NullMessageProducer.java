package no.nav.kafka.sandbox.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class NullMessageProducer {

    private final Map<String, Object> kafkaSettings;
    private final String topic;
    private final Integer partition;
    private final Function<Object, String> keyFunction;

    private static final Logger log = LoggerFactory.getLogger(NullMessageProducer.class);

    public NullMessageProducer(String topic, Integer partition, Map<String,Object> kafkaSettings, Function<Object, String> keyFunction) {
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
        this.kafkaSettings = Objects.requireNonNull(kafkaSettings);
        this.keyFunction = Objects.requireNonNull(keyFunction);
    }

    public void produce() {
        final KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(kafkaSettings);
        final String key = keyFunction.apply(null);

        try {
            RecordMetadata recordMetadata = kafkaProducer.send(new ProducerRecord<>(topic, partition, key, null)).get();
            log.info("Sent a null message to {}-{} with key {}, record offset={}, timestamp={}",
                    recordMetadata.topic(), recordMetadata.partition(), key, recordMetadata.offset(), recordMetadata.timestamp());
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
            log.error("Error sending to Kafka", e);
        }

        kafkaProducer.close();
    }

}
