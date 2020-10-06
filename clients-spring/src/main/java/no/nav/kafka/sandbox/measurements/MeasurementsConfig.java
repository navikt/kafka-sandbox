package no.nav.kafka.sandbox.measurements;

import no.nav.kafka.sandbox.data.DefaultEventStore;
import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.Measurements;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.ParseStringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Example of multi threaded Kafka consumer setup using Spring, with manual setup of container listener factory and
 * consumer factory.
 */
@Configuration
public class MeasurementsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * In-memory store of measurement events received from Kafka.
     */
    @Bean
    public EventStore<Measurements.SensorEvent> sensorEventStore(@Value("${measurements.event-store.max-size:200}") int maxSize) {
        return new DefaultEventStore<>(maxSize);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Measurements.SensorEvent> measurementsListenerContainer() {
        ConcurrentKafkaListenerContainerFactory<String, Measurements.SensorEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2); // Decrease/increase to observe how many threads are invoking the listener endpoint with message batches
        factory.setAutoStartup(true);
        factory.setBatchListener(true);
        return factory;
    }

    private DefaultKafkaConsumerFactory<String, Measurements.SensorEvent> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps(),
                new StringDeserializer(),
                new JsonDeserializer<>(Measurements.SensorEvent.class));
    }

    private Map<String, Object> consumerProps() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "spring-web-measurements",
                ConsumerConfig.CLIENT_ID_CONFIG, "spring-web-measurements");
    }

}
