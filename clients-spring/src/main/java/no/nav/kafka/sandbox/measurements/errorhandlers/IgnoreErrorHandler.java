package no.nav.kafka.sandbox.measurements.errorhandlers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ContainerAwareBatchErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Custom error handler that logs, but ignores errors, letting Spring commit offsets.
 */
public class IgnoreErrorHandler implements ContainerAwareBatchErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(IgnoreErrorHandler.class);

    @Override
    public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
        LOG.error("Kafka consumer failed to process batch of {} records with {}: {}", data.count(),
                thrownException.getClass().getSimpleName(), thrownException.getMessage());
    }

}
