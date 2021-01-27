package no.nav.kafka.sandbox.consolemessages;

import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.ConsoleMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Demonstrates simple configuration of a consumer using only defaults from Spring auto-configuration and external
 * configuration file <code>src/main/resources/application.yml</code>. Annotation overrides the minimal set of properties
 * to allow proper deserialization of the incoming message type.
 *
 * <p>The consumer can only process one record at a time.</p>
 */
@Component
public class ConsoleMessagesConsumer {

    private final EventStore<ConsoleMessages.Message> store;

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleMessagesConsumer.class);

    public ConsoleMessagesConsumer(EventStore<ConsoleMessages.Message> messageEventStore) {
        this.store = messageEventStore;
    }

    @KafkaListener(
            topics = "${consolemessages.consumer.topic}",
            properties = {"spring.json.value.default.type=no.nav.kafka.sandbox.messages.ConsoleMessages.Message"})
    public void receiveMessage(ConsoleMessages.Message message) {
        if (message == null) {
            throw new NullPointerException("Message was null");
        }

        LOG.info("Received a console message from {}", message.senderId);
        store.storeEvent(message);
    }

}
