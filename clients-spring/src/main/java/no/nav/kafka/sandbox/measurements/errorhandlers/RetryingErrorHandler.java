package no.nav.kafka.sandbox.measurements.errorhandlers;

import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.Measurements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.RetryingBatchErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;

public class RetryingErrorHandler extends RetryingBatchErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingErrorHandler.class);

    public RetryingErrorHandler(EventStore<Measurements.SensorEvent> store) {
        super(new FixedBackOff(2000L, 2), (record, exception) -> {
            Throwable cause = exception;
            if (exception instanceof ListenerExecutionFailedException) {
                cause = exception.getCause();
            }

            if (cause instanceof IOException) {
                // Something has gone wrong writing to event store, and here we assume it is a transient error condition and would
                // like the whole batch to be re-processed.
                throw new RuntimeException("Event store batch failure, retrying the whole batch", cause);
            }

            if (cause instanceof NullPointerException) { // Or any expected business level type which can be sensibly handled
                // Some message in batch failed because of null value
                if (record.value() == null) {
                    LOG.error("Record at {}-{} offset {} has null value, skipping it", record.topic(), record.partition(), record.offset());
                    return;
                }

                if (!(record.value() instanceof Measurements.SensorEvent)) {
                    LOG.error("Record at {}-{} offset {} has invalid message type, skipping it", record.topic(), record.partition(), record.offset());
                    return;
                }

                // This message however was not null, so we try to ensure it gets written into the event store.
                Measurements.SensorEvent recoveredEvent = (Measurements.SensorEvent) record.value();
                if (store.storeEvent(recoveredEvent)) { // Any exception thrown from store here will cause the whole batch to be re-processed.
                    LOG.info("Recovered and stored event at {}-{} offset {}", record.topic(), record.partition(), record.offset());
                }
                return;
            }

            // We have no known way to handle this, so we let the whole batch be re-processed.
            // Depending on business requirements (e.g. if not at-least-once semantics), then another strategy might
            // be to skip the whole batch, let Spring commit offsets and continue with the next instead.
            throw new RuntimeException("Unrecoverable batch error", cause);
        });
    }

}
