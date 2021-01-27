package no.nav.kafka.sandbox.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Console;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ConsoleMessages {

    public static class Message {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Message message = (Message) o;
            return Objects.equals(text, message.text) && Objects.equals(senderId, message.senderId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text, senderId);
        }
    }

    public static Consumer<Message> consoleMessageConsumer() {
        return m -> {
            System.out.println(m.senderId + ": " + m.text);
        };
    }

    public static Supplier<Message> consoleMessageSupplier() {
        final Console console = System.console();
        if (console == null) {
            throw new IllegalStateException("Unable to get system console");
        }
        final String senderId = "sender-" + ProcessHandle.current().pid();
        final AtomicBoolean first = new AtomicBoolean(true);
        return () -> {
            if (first.getAndSet(false)) {
                console.writer().println("Send messages to Kafka, use CTRL+D to exit gracefully.");
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return new Message("interrupted", senderId);
            }
            console.writer().print("Type message> ");
            console.writer().flush();
            String text = console.readLine();
            if (text == null){
                Thread.currentThread().interrupt();
                return new Message("leaving", senderId);
            } else {
                return new Message(text, senderId);
            }
        };
    }

}
