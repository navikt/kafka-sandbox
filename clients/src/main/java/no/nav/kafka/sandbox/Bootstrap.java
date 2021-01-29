package no.nav.kafka.sandbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.nav.kafka.sandbox.admin.TopicAdmin;
import no.nav.kafka.sandbox.consumer.JsonMessageConsumer;
import no.nav.kafka.sandbox.messages.ConsoleMessages;
import no.nav.kafka.sandbox.messages.Measurements;
import no.nav.kafka.sandbox.messages.SequenceValidation;
import no.nav.kafka.sandbox.producer.JsonMessageProducer;
import no.nav.kafka.sandbox.producer.NullMessageProducer;
import no.nav.kafka.sandbox.producer.StringMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A simple Kafka Java producer/consumer demo app with minimized set of dependencies.
 *
 * <p>Purpose:</p>
 * <ol>
 *     <li>Get quickly up and running with Kafka using standard Java Kafka client.</li>
 *     <li>Experiment with the settings to learn and understand behaviour.</li>
 *     <li>Experiment with the console clients to learn about communication patterns possible with Kafka, and
 *     how topic partitions and consumer groups work in practice.</li>
 *     <li>Easily modify and re-run code in the experimentation process.</li>
 *     <li>Create unit/integration tests that use Kafka.</li>
 * </ol>
 */
public class Bootstrap {

    final static String DEFAULT_BROKER = "localhost:9092";
    final static String MEASUREMENTS_TOPIC = "measurements";
    final static String MESSAGES_TOPIC = "messages";
    final static String SEQUENCE_TOPIC = "sequence";
    final static String CONSUMER_GROUP_DEFAULT = "console";

    private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String...a) {
        final LinkedList<String> args = new LinkedList(Arrays.asList(a));

        if (args.isEmpty() || args.get(0).isBlank() || args.contains("-h") || args.get(0).contains("--help")) {
            System.err.println("Use: 'producer [TOPIC [P]]' or 'consumer [TOPIC [GROUP]]'");
            System.err.println("Use: 'console-message-producer [TOPIC [P]]' or 'console-message-consumer [TOPIC [GROUP]]'");
            System.err.println("Use: 'sequence-producer [TOPIC [P]]' or 'sequence-consumer [TOPIC [GROUP]]'");
            System.err.println("Use: 'null-producer [TOPIC [P]]' to produce a single message with null value");
            System.err.println("Use: 'string-producer STRING [TOPIC [P]]' to produce a single UTF-8 encoded string message");
            System.err.println("Use: 'newtopic TOPIC [N]' to create a topic with N partitions (default 1).");
            System.err.println("Use: 'deltopic TOPIC' to delete a topic.");
            System.err.println("Use: 'showtopics' to list topics/partitions available.");
            System.err.println("Default topic is chosen according to consumer/producer type.");
            System.err.println("Default consumer group is '"+ CONSUMER_GROUP_DEFAULT + "'");
            System.err.println("Kafka broker is " + DEFAULT_BROKER);
            System.exit(1);
        }

        try {
            switch (args.remove()) {
                case "newtopic":
                    newTopic(args);
                    break;

                case "deltopic":
                    deleteTopic(args);
                    break;

                case "showtopics":
                    showTopics(args);
                    break;

                case "producer":
                    measurementProducer(args);
                    break;

                case "consumer":
                    measurementConsumer(args);
                    break;

                case "sequence-producer":
                    sequenceProducer(args);
                    break;

                case "null-producer":
                    nullProducer(args);
                    break;

                case "string-producer":
                    stringProducer(args);
                    break;

                case "sequence-consumer":
                    sequenceValidatorConsumer(args);
                    break;

                case "console-message-producer":
                    consoleMessageProducer(args);
                    break;

                case "console-message-consumer":
                    consoleMessageConsumer(args);
                    break;

                default:
                    System.err.println("Invalid mode");
                    System.exit(1);
            }
        } catch (IllegalArgumentException | NoSuchElementException e) {
            System.err.println("Bad syntax");
            System.exit(1);
        }
    }

    private static void showTopics(Queue<String> args) {
        try (TopicAdmin ta = new TopicAdmin(DEFAULT_BROKER)) {
            LOG.info("Topic-partitions available at broker {}:", DEFAULT_BROKER);
            ta.listTopics().stream().sorted().forEach(t -> System.out.println(t));
        } catch (Exception e) {
            System.err.println("Failed: "+ e.getMessage());
        }
    }

    private static void newTopic(Queue<String> args) {
        try (TopicAdmin ta = new TopicAdmin(DEFAULT_BROKER)) {
            String topic = args.remove();
            int partitions = args.isEmpty() ? 1 : Integer.parseInt(args.remove());
            ta.create(topic, partitions);
            LOG.info("New topic '{}' created with {} partitions.", topic, partitions);
        } catch (Exception e) {
            System.err.println("Failed: "+ e.getMessage());
        }
    }

    private static void deleteTopic(Queue<String> args) {
        try (TopicAdmin ta = new TopicAdmin(DEFAULT_BROKER)) {
            String topic = args.remove();
            ta.delete(topic);
            LOG.info("Delete topic '{}'", topic);
        } catch (Exception e) {
            System.err.println("Failed: "+ e.getMessage());
        }
    }

    private static void measurementProducer(Queue<String> args) {
        String topic = args.isEmpty() ? MEASUREMENTS_TOPIC : args.remove();
        Integer partition = args.isEmpty() ? null : Integer.parseInt(args.remove());
        jsonProducer(topic, partition, Measurements::acquireTemperatureSensorMeasurement, m -> m.getDeviceId());
    }

    private static void measurementConsumer(Queue<String> args) {
        String topic = args.isEmpty() ? MEASUREMENTS_TOPIC : args.remove();
        String group = args.isEmpty() ? CONSUMER_GROUP_DEFAULT : args.remove();
        jsonConsumer(topic, group, Measurements.SensorEvent.class, Measurements::sensorEventToConsole);
    }

    private static void consoleMessageProducer(Queue<String> args) {
        String topic = args.isEmpty() ? MESSAGES_TOPIC : args.remove();
        Integer partition = args.isEmpty() ? null : Integer.parseInt(args.remove());
        jsonProducer(topic, partition, ConsoleMessages.consoleMessageSupplier(), m -> m.senderId);
    }

    private static void consoleMessageConsumer(Queue<String> args) {
        String topic = args.isEmpty() ? MESSAGES_TOPIC : args.remove();
        String group = args.isEmpty() ? CONSUMER_GROUP_DEFAULT : args.remove();
        jsonConsumer(topic, group, ConsoleMessages.Message.class, ConsoleMessages.consoleMessageConsumer());
    }

    private static void sequenceProducer(Queue<String> args) {
        String topic = args.isEmpty() ? SEQUENCE_TOPIC : args.remove();
        Integer partition = args.isEmpty() ? null : Integer.parseInt(args.remove());
        jsonProducer(topic, partition, SequenceValidation.sequenceSupplier(
                new File("target/sequence-producer.state"), 1, TimeUnit.SECONDS), m -> null);
    }

    private static void sequenceValidatorConsumer(Queue<String> args) {
        String topic = args.isEmpty() ? SEQUENCE_TOPIC : args.remove();
        String group = args.isEmpty() ? CONSUMER_GROUP_DEFAULT : args.remove();
        jsonConsumer(topic, group, Long.class, SequenceValidation.sequenceValidatorConsolePrinter());
    }

    private static void nullProducer(Queue<String> args) {
        String topic = args.isEmpty() ? MEASUREMENTS_TOPIC : args.remove();
        Integer partition = args.isEmpty() ? null : Integer.parseInt(args.remove());
        new NullMessageProducer(topic, partition, KafkaConfig.kafkaProducerProps(), m -> null)
                .produce();
    }

    private static void stringProducer(Queue<String> args) {
        String message = Objects.requireNonNull(args.remove(), "Message cannot be null");
        String topic = args.isEmpty() ? MEASUREMENTS_TOPIC : args.remove();
        Integer partition = args.isEmpty() ? null : Integer.parseInt(args.remove());
        new StringMessageProducer(topic, partition, KafkaConfig.kafkaProducerProps(), message,
                m -> String.valueOf(Math.abs(m.hashCode())))
                .produce();
    }

    private static <M> void jsonProducer(String topic, Integer partition, Supplier<M> messageSupplier, Function<M, String> keyFunction) {
        LOG.info("New producer with PID " + obtainPid());
        JsonMessageProducer<M> producer = new JsonMessageProducer<>(topic, partition, KafkaConfig.kafkaProducerProps(), objectMapper(),
                messageSupplier, keyFunction, true);

        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            main.interrupt();
            try { main.join(2000); } catch (InterruptedException ie){ }
        }));

        producer.produceLoop();
    }

    private static <M> void jsonConsumer(String topic, String group, Class<M> messageType, Consumer<M> messageHandler) {
        LOG.info("New consumer with PID " + obtainPid());
        JsonMessageConsumer<M> consumer =
                new JsonMessageConsumer(topic, messageType, KafkaConfig.kafkaConsumerProps(group),
                        objectMapper(), messageHandler);
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Cannot directly close KafkaConsumer in shutdown hook,
            // since this code runs in another thread, and KafkaConsumer complains loudly if accessed by multiple threads
            main.interrupt();
            try { main.join(2000); } catch (InterruptedException ie){ }
        }));

        consumer.consumeLoop();
    }

    static long obtainPid() {
        return ProcessHandle.current().pid();
    }

    static ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

}
