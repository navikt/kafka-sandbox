package no.nav.kafka.sandbox.measurements;

import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.Measurements;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
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

    public MeasurementsConsumer(EventStore<Measurements.SensorEvent> store,
                                @Value("${measurements.consumer.slowdown:0}") long slowdownMillis) {
        this.eventStore = store;
        this.slowdownMillis = slowdownMillis;
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
        records.forEach(r -> eventStore.storeEvent(r.value()));
    }

}
