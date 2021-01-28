package no.nav.kafka.sandbox.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Expects JSON-formatted messages, deserializes using specific type and passes message objects to provided handler
 * as they come in from Kafka.
 *
 * <p>Message handler does not need to be thread safe unless shared with other Kafka consumer clients.</p>
 */
public class JsonMessageConsumer<T> {

    private final ObjectMapper mapper;
    private final String topic;
    private Map<String,Object> kafkaConfig;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Class<T> messageType;
    private final Consumer<T> messageHandler;
    private final AtomicInteger successConsumeCount = new AtomicInteger(0);

    public JsonMessageConsumer(String topic, Class<T> messageType, Map<String,Object> kafkaConfig,
                               ObjectMapper mapper, Consumer<T> messageHandler) {
        this.mapper = mapper;
        this.messageType = messageType;
        this.topic = topic;
        this.kafkaConfig = kafkaConfig;
        this.messageHandler = messageHandler;
    }

    private KafkaConsumer<String,String> initKafkaConsumer(String topic,  Map<String,Object> kafkaConfig) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConfig);
        log.debug("Subscribe to topic '{}' as part of consumer group '{}'", topic, kafkaConfig.get(ConsumerConfig.GROUP_ID_CONFIG));

        // Subscribe, which uses auto-partition assignment (Kafka consumer-group balancing)
        consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitions.forEach(tp -> {
                    log.info("Rebalance: no longer assigned to topic {}, partition {}", tp.topic(), tp.partition());
                });
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(tp -> {
                    log.info("Rebalance: assigned to topic {}, partition {}", tp.topic(), tp.partition());
                });
            }
        });

        // Another way would be to explicitly assign a partition to this consumer, by topic and number
        //consumer.assign(Collections.singleton(new TopicPartition(topic, 0)));
        //log.info("consumer assigned partition '{}'", waitForAssignment(consumer));
        return consumer;
    }

    /**
     * Initialize Kafka consumer, enter consume loop, run until interrupted, then close Kafka consumer.
     */
    public void consumeLoop() {
        KafkaConsumer<String,String> kafkaConsumer = initKafkaConsumer(topic, kafkaConfig);
        log.info("Start consumer loop");
        while (!Thread.interrupted()) {
            log.info("Poll ..");
            try {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(10));
                if (records.count() > 0) {
                    log.info("Total records count fetched: {}, partitions: {}", records.count(), records.partitions());

                    records.records(topic).forEach(this::handleRecord);

                    // Could avoid commit if handler failed for one or more records here, then seek to start of
                    // Not needed if using auto-commit strategy, see Kafka consumer config.
                    kafkaConsumer.commitSync();
                    records.partitions().forEach(tp -> {
                        log.info("Committed offset {} for {}",
                                kafkaConsumer.committed(Collections.singleton(tp)).get(tp).offset(), tp);
                    });
                }
            } catch (InterruptException ie) { // Note: Kafka-specific interrupt exception
                // Expected on shutdown from console
            } catch (KafkaException ke) {
                log.error("KafkaException occured in consumeLoop", ke);
            }
        }
        log.info("Closing KafkaConsumer ..");
        kafkaConsumer.close();
        log.info("Successfully consumed {} messages.", successConsumeCount.get());
    }

    private void handleRecord(ConsumerRecord<String,String> record) {
        try {
            T value = deserialize(record.value(), this.messageType);
            this.messageHandler.accept(value);
            successConsumeCount.incrementAndGet();
        } catch (Exception e) {
            log.error("Handle record with offset {}, failed to in message handler or deserialization: {}" , record.offset(), e.getMessage());
        }
    }

    private T deserialize(String json, Class<T> messageType) throws Exception {
        return mapper.readValue(json, messageType);
    }

}
