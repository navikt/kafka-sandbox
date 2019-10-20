package no.nav.kafka.sandbox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
    @JsonProperty
    public final String text;
    @JsonProperty
    public final String senderId;

    @JsonCreator
    public Message(@JsonProperty("text") String text,
                   @JsonProperty("senderId") String id) {
        this.text = text;
        this.senderId = id;
    }
}
