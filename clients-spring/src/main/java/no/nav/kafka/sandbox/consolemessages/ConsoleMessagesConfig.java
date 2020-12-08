package no.nav.kafka.sandbox.consolemessages;

import no.nav.kafka.sandbox.data.DefaultEventStore;
import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.ConsoleMessages;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsoleMessagesConfig {

    /**
     * In-memory store of console messages received from Kafka.
     */
    @Bean
    public EventStore<ConsoleMessages.Message> messageEventStore(@Value("${consolemessages.event-store.max-size:200}") int maxSize) {
        return new DefaultEventStore<>(maxSize, false);
    }

}
