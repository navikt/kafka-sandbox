package no.nav.kafka.sandbox.consolemessages;

import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.ConsoleMessages;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

@RestController
public class ConsoleMessagesRestController {

    private final EventStore<ConsoleMessages.Message> messageStore;

    public ConsoleMessagesRestController(EventStore<ConsoleMessages.Message> messageEventStore) {
        this.messageStore = messageEventStore;
    }

    /**
     * Get messages ordered ascending by time of reception.
     */
    @GetMapping(value = "/messages/api", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ConsoleMessages.Message> getMessages() {
        return messageStore.fetchEvents();
    }

}
