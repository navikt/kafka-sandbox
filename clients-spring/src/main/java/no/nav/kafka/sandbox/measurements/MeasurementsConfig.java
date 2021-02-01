package no.nav.kafka.sandbox.measurements;

import no.nav.kafka.sandbox.data.DefaultEventStore;
import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.data.EventStoreWithFailureRate;
import no.nav.kafka.sandbox.measurements.errorhandlers.IgnoreErrorHandler;
import no.nav.kafka.sandbox.measurements.errorhandlers.RecoveringErrorHandler;
import no.nav.kafka.sandbox.measurements.errorhandlers.RetryingErrorHandler;
import no.nav.kafka.sandbox.messages.Measurements.SensorEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Example of multi threaded Kafka consumer setup using Spring, with manual setup of container listener factory and
 * consumer factory.
 *
 * <p>The configuration of Kafka consumer aspects is based on config from {@code src/main/resources/application.yml} with specific
 * overrides done manually in this configuration class.</p>
 *
 * <p>Error handling strategies can be customized by setting value of property <code>measurements.consumer.error-handler</code>
 * </p>.
 */
@Configuration
public class MeasurementsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MeasurementsConfig.class);

    /**
     * Customize different exceptions to throw depending on event to store.
     */
    @Bean
    public Function<SensorEvent, Throwable> fakeExceptionSupplier() {
        return event -> new IOException("Failed to store event " + event.toString() + ", general I/O failure");
    }

    /**
     * In-memory store of measurement events received from Kafka.
     */
    @Bean
    public EventStore<SensorEvent> sensorEventStore(@Value("${measurements.event-store.max-size:200}") int maxSize,
                                                    @Value("${measurements.event-store.failure-rate:0.0}") float failureRate,
                                                    @Value("${measurements.event-store.fail-on-max-size:false}") boolean failOnMaxSize,
                                                    Function<SensorEvent, Throwable> exceptionSupplier) {
        if (failureRate > 0) {
            return new EventStoreWithFailureRate<>(maxSize, failOnMaxSize, failureRate, exceptionSupplier);
        } else {
            return new DefaultEventStore<>(maxSize, failOnMaxSize);
        }
    }

    /**
     * @return a Kafka listener container, which can be referenced by name from listener endpoints.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SensorEvent> measurementsListenerContainer(
            ConsumerFactory<String, SensorEvent> consumerFactory,
            Optional<BatchErrorHandler> errorHandler,
            @Value("${measurements.consumer.handle-deserialization-error:true}") boolean handleDeserializationError) {

        // Consumer configuration from application.yml, where we will override some properties:
        Map<String, Object> externalConfigConsumerProps = new HashMap<>(consumerFactory.getConfigurationProperties());

        ConcurrentKafkaListenerContainerFactory<String, SensorEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(externalConfigConsumerProps, handleDeserializationError));
        factory.setConcurrency(2); // Decrease/increase to observe how many threads are invoking the listener endpoint with message batches
        factory.setAutoStartup(true);
        factory.setBatchListener(true);

        // See https://docs.spring.io/spring-kafka/reference/html/#committing-offsets:
        // BATCH is default, but included here for clarity. It controls how offsets are commited back to Kafka.
        // Spring will commit offsets when a batch listener has completed processing without any exceptions being thrown.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        if (errorHandler.isPresent()) {
            LOG.info("Using error handler: {}", errorHandler.get().getClass().getSimpleName());
            factory.setBatchErrorHandler(errorHandler.get());
        } else {
            LOG.info("Using Spring Kafka default error handler");
        }

        if (handleDeserializationError) {
            LOG.info("Will handle value deserialization errors.");
        } else {
            LOG.info("Value deserialization errors are not handled explicitly.");
        }

        return factory;
    }

    private DefaultKafkaConsumerFactory<String, SensorEvent> consumerFactory(
            Map<String,Object> externalConfigConsumerProps,
            boolean handleDeserializationError) {
        // override some consumer props from external config
        Map<String, Object> consumerProps = new HashMap<>(externalConfigConsumerProps);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "spring-web-measurement");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "spring-web-measurement");

        // Deserialization config
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, SensorEvent.class.getName());

        if (handleDeserializationError) {
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            consumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        }

        return new DefaultKafkaConsumerFactory<>(consumerProps);
    }

    @Bean
    @ConditionalOnProperty(value = "measurements.consumer.error-handler", havingValue = "ignore")
    public BatchErrorHandler ignoreHandler() {
        return new IgnoreErrorHandler();
    }

    @Bean
    @ConditionalOnProperty(value = "measurements.consumer.error-handler", havingValue = "seek-to-current")
    public BatchErrorHandler seekToCurrentHandler() {
        return new SeekToCurrentBatchErrorHandler();
    }

    @Bean
    @ConditionalOnProperty(value = "measurements.consumer.error-handler", havingValue = "seek-to-current-with-backoff")
    public BatchErrorHandler seekToCurrentWithBackoffHandler() {
        SeekToCurrentBatchErrorHandler handler = new SeekToCurrentBatchErrorHandler();
        // For this error handler, max attempts actually does not matter
        handler.setBackOff(new FixedBackOff(2000L, 2));
        return handler;
    }

    @Bean
    @ConditionalOnProperty(value = "measurements.consumer.error-handler", havingValue = "retry-with-backoff")
    public BatchErrorHandler retryWithBackoffHandler() {
        return new RetryingBatchErrorHandler(new FixedBackOff(2000L, 2), null);
    }

    @Bean
    @ConditionalOnProperty(value = "measurements.consumer.error-handler", havingValue = "retry-with-backoff-recovery")
    public BatchErrorHandler retryWithBackoffRecoveryHandler(EventStore<SensorEvent> eventStore) {
        return new RetryingErrorHandler(eventStore);
    }

    @Bean
    @ConditionalOnProperty(value = "measurements.consumer.error-handler", havingValue = "recovering")
    public BatchErrorHandler recoveringHandler() {
        return new RecoveringErrorHandler();
    }

    @Bean
    @ConditionalOnProperty(value = "measurements.consumer.error-handler", havingValue = "stop-container")
    public BatchErrorHandler containerStoppingHandler() {
        return new ContainerStoppingBatchErrorHandler();
    }

}
