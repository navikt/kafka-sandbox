package no.nav.kafka.sandbox.measurements;

import no.nav.kafka.sandbox.data.DefaultEventStore;
import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.data.EventStoreWithFailureRate;
import no.nav.kafka.sandbox.messages.Measurements.SensorEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Example of multi threaded Kafka consumer setup using Spring, with manual setup of container listener factory and
 * consumer factory.
 *
 * <p>The configuration of Kafka consumer aspects is based on config from {@code src/main/resources/application.yml} with specific
 * overrides done manually in this configuration class.</p>
 */
@Configuration
public class MeasurementsConfig {

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
     * @param enableCustomErrorHandler whether to configure a custom error handler for the listener container
     * @return a Kafka listener container, which can be referenced by name from listener endpoints.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SensorEvent> measurementsListenerContainer(
            ConsumerFactory<String, SensorEvent> consumerFactory, // Provided by Spring
            @Value("${measurements.consumer.error-handler:spring-default}") String errorHandler) {

        // Consumer configuration from application.yml, where we will override some properties:
        Map<String, Object> externalConfigConsumerProps = new HashMap<>(consumerFactory.getConfigurationProperties());


        ConcurrentKafkaListenerContainerFactory<String, SensorEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(externalConfigConsumerProps));
        factory.setConcurrency(2); // Decrease/increase to observe how many threads are invoking the listener endpoint with message batches
        factory.setAutoStartup(true);
        factory.setBatchListener(true);

        // See https://docs.spring.io/spring-kafka/reference/html/#committing-offsets:
        // BATCH is default, but included here for clarity. It controls how offsets are commited back to Kafka.
        // Spring will commit offsets when a batch listener has completed processing without any exceptions being thrown.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        if ("ignore-errors".equals(errorHandler)) {
            factory.setBatchErrorHandler(new IgnoreErrorsHandler());
        }

        return factory;
    }

    private DefaultKafkaConsumerFactory<String, SensorEvent> consumerFactory(Map<String,Object> externalConfigConsumerProps) {
        // override some consumer props from external config
        Map<String, Object> consumerProps = new HashMap<>(externalConfigConsumerProps);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "spring-web-measurement");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "spring-web-measurement");

        return new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(),
                new JsonDeserializer<>(SensorEvent.class));
    }

}
