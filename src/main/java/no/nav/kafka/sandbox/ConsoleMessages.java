package no.nav.kafka.sandbox;

import java.io.Console;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

class ConsoleMessages {

    static Class<Message> messageClass = Message.class;

    static Consumer<Message> consoleMessageConsumer() {
        return (m) -> {
            System.out.println(m.senderId + ": " + m.text);
        };
    }

    static Supplier<Message> consoleMessageSupplier() {
        final Console console = System.console();
        if (console == null) {
            throw new IllegalStateException("Unable to get system console");
        }
        final String senderId = "sender-" + Bootstrap.obtainPid();
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
