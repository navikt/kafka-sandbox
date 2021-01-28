package no.nav.kafka.sandbox.measurements;

import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.measurements.errorhandlers.RecoveringErrorHandler;
import no.nav.kafka.sandbox.messages.Measurements;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Example of more advanced multi threaded Kafka consumer setup using Spring. The listener endpoint here references a custom built
 * {@link org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory} in {@link MeasurementsConfig} by
 * use of annotation attribute <code>containerFactory</code>.
 *
 * <p>Demonstrates listener endpoint receiving batches of consumer records.</p>
 *
 * <p>You can increase slowdown to get larger batches each time the endpoint is called.</p>
 *
 */
@Component
public class MeasurementsConsumer {

    private final EventStore<Measurements.SensorEvent> eventStore;

    private static final Logger LOG = LoggerFactory.getLogger(MeasurementsConsumer.class);

    private final long slowdownMillis;

    private final boolean usingRecoveringBatchErrorHandler;

    public MeasurementsConsumer(EventStore<Measurements.SensorEvent> store,
                                @Value("${measurements.consumer.slowdown:0}") long slowdownMillis,
                                Optional<BatchErrorHandler> errorHandler) {
        this.eventStore = store;
        this.slowdownMillis = slowdownMillis;
        this.usingRecoveringBatchErrorHandler = errorHandler.isPresent() && errorHandler.get() instanceof RecoveringErrorHandler;
    }

    /**
     * More Kafka config in {@link MeasurementsConfig}.
     * @param records
     */
    @KafkaListener(topics = "${measurements.consumer.topic}", containerFactory = "measurementsListenerContainer")
    public void receive(List<ConsumerRecord<String, Measurements.SensorEvent>> records) {
        LOG.info("Received list of {} Kafka consumer records", records.size());

        if (slowdownMillis > 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(slowdownMillis);
            } catch (InterruptedException ie) {}
        }

        records.forEach(record -> {
            if (checkFailedDeserialization(record)) return;

            if (record.value() == null) {
                NullPointerException businessException = new NullPointerException("Message at "
                        + record.topic() + "-" + record.partition() + ":" + record.offset() + " with key " + record.key() + " was null");

                if (usingRecoveringBatchErrorHandler) {
                    // Communicate to recovering batch error handler which record in the batch that failed, and the root cause
                    throw new BatchListenerFailedException(businessException.getMessage(), businessException, record);
                } else {
                    // Throw raw root cause for other types of error handling
                    throw businessException;
                }
            }

            try {
                eventStore.storeEvent(record.value());
            } catch (Exception e) {
                if (usingRecoveringBatchErrorHandler) {
                    throw new BatchListenerFailedException(e.getMessage(), e, record);
                } else {
                    throw e;
                }
            }
        });
    }

    private boolean checkFailedDeserialization(ConsumerRecord<String, Measurements.SensorEvent> record) {
        return failedValueDeserialization(record).map(e -> {
            LOG.error("Message value at {}-{} offset {} failed to deserialize: {}: {}, skipping it.",
                    record.topic(), record.partition(), record.offset(),
                    e.getClass().getSimpleName(), e.getMessage());
            return true;
        }).orElse(false);
    }


    private static Optional<Throwable> failedValueDeserialization(ConsumerRecord<String, Measurements.SensorEvent> record) {
        Header valueDeserializationError = record.headers().lastHeader(ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        if (valueDeserializationError != null) {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(valueDeserializationError.value()))){
                DeserializationException dex = (DeserializationException)ois.readObject();
                return Optional.ofNullable(dex);
            } catch (Exception e) {
                LOG.error("Failed to read header containing deserialization exception information at {}-{}, offset {}",
                        record.topic(), record.partition(), record.offset());
            }
        }

        return Optional.empty();
    }


}
