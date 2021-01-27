package no.nav.kafka.sandbox.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class JsonMessageProducer<T> {

    private final ObjectMapper mapper;
    private final Map<String,Object> kafkaSettings;
    private final String topic;
    private static final Logger log = LoggerFactory.getLogger(JsonMessageProducer.class);
    private final Supplier<T> messageSupplier;
    private final Function<T, String> keyFunction;
    private final Integer partition;
    private final boolean nonBlockingSend;

    /**
     *
     * @param topic Kakfa topic to send to
     * @param partition desired partition, or {@code null} for selection based on message key
     * @param kafkaSettings
     * @param mapper
     * @param messageSupplier supplier of messages
     * @param keyFunction function to derive a key from a message instance. The function may return {@code null} if no
     *                    key is desired.
     * @param nonBlockingSend whether to do non-blocking send or wait for each ack
     */
    public JsonMessageProducer(String topic, Integer partition, Map<String,Object> kafkaSettings, ObjectMapper mapper,
                               Supplier<T> messageSupplier, Function<T, String> keyFunction,
                               boolean nonBlockingSend) {
        this.mapper = mapper;
        this.kafkaSettings = kafkaSettings;
        this.topic = topic;
        this.partition = partition;
        this.messageSupplier = messageSupplier;
        this.keyFunction = keyFunction;
        this.nonBlockingSend = nonBlockingSend;
    }

    /**
     * Send as fast as the supplier can generate messages, until interrupted, then close producer
     */
    public void produceLoop() {
        log.info("Start producer loop");
        final KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(kafkaSettings);
        final SendStrategy<T> sendStrategy = nonBlockingSend ? nonBlocking(kafkaProducer) : blocking(kafkaProducer);

        final AtomicInteger sendCount = new AtomicInteger(0);
        while (!Thread.interrupted()) {
            try {
                T message = messageSupplier.get();
                String key = keyFunction.apply(message);

                sendStrategy.send(key, message, recordMetadata -> sendCount.incrementAndGet());
            } catch (InterruptException kafkaInterrupt) {
                // Expected on shutdown from console
            } catch (Exception ex) {
                log.error("Unexpected error when sending to Kafka", ex);
            }
        }
        kafkaProducer.close();
        log.info("Sent in total {} messages", sendCount.get());
    }

    @FunctionalInterface
    interface SendStrategy<T> {
        void send(String key, T message, Consumer<RecordMetadata> successCallback) throws Exception;
    }

    /**
     * Non-blocking send, generally does not block, prints status in callback invoked by Kafka producer
     */
    private SendStrategy<T> nonBlocking(KafkaProducer<String,String> kafkaProducer) {
        return (String key, T message, Consumer<RecordMetadata> successCallback) -> {
            final String json = mapper.writeValueAsString(message);
            log.debug("Send non-blocking ..");
            kafkaProducer.send(new ProducerRecord<>(topic, partition, key, json), (metadata, ex) -> {
                if (ex != null) {
                    log.error("Failed to send message to Kafka", ex);
                } else {
                    log.debug("Async message ack, offset: {}, timestamp: {}, topic-partition: {}-{}",
                            metadata.offset(), metadata.timestamp(), metadata.topic(), metadata.partition());
                    successCallback.accept(metadata);
                }
            });
        };
    }

    /**
     * Blocking send strategy, waits for result of sending, then prints status and returns
     */
    private SendStrategy<T> blocking(KafkaProducer<String, String> kafkaProducer) {
        return (String key, T message, Consumer<RecordMetadata> successCallback) -> {
            String json = mapper.writeValueAsString(message);
            log.debug("Send blocking ..");
            Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(topic, partition, key, json));
            try {
                RecordMetadata metadata = send.get();
                log.debug("Message ack, offset: {}, timestamp: {}, topic-partition: {}-{}",
                        metadata.offset(), metadata.timestamp(), metadata.topic(), metadata.partition());
                successCallback.accept(metadata);
            } catch (ExecutionException exception) {
                log.error("Failed to send message to Kafka", exception.getCause());
            }
        };
    }

}
