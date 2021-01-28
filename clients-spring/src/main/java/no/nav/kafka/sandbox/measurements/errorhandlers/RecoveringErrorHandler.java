package no.nav.kafka.sandbox.measurements.errorhandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.RecoveringBatchErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

public class RecoveringErrorHandler extends RecoveringBatchErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveringErrorHandler.class);

    public RecoveringErrorHandler() {
        super((record, exception) -> {
            Throwable cause = exception;
            if (exception instanceof ListenerExecutionFailedException) {
                cause = ((ListenerExecutionFailedException)exception).getRootCause();
            }

            if (cause instanceof NullPointerException && record.value() == null) {
                // We know how to handle this
                LOG.error("Discarding null message at {}-{} offset {}", record.topic(), record.partition(), record.offset());
                return;
            }

            throw new RuntimeException("Unable to recover from exception, retry the rest of the batch.", cause);
        }, new FixedBackOff(2000L, 2));
    }

}
