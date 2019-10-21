package no.nav.kafka.sandbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.nav.kafka.sandbox.admin.TopicAdmin;
import no.nav.kafka.sandbox.consumer.JsonMessageConsumer;
import no.nav.kafka.sandbox.producer.JsonMessageProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
    final static String CONSUMER_GROUP_DEFAULT = "console";

    private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String...a) {
        final LinkedList<String> args = new LinkedList(Arrays.asList(a));

        if (args.isEmpty() || args.get(0).isBlank() || "-h".equals(args.get(0)) || "--help".equals(args.get(0))) {
            System.err.println("Use: 'producer [TOPIC [P]]' or 'consumer [TOPIC [GROUP]]'");
            System.err.println("Use: 'console-message-producer [TOPIC [P]]' or 'console-message-consumer [TOPIC [GROUP]]'");
            System.err.println("Use: 'newtopic TOPIC [N]' to create a topic with N partitions (default 1).");
            System.err.println("Use: 'deltopic TOPIC' to delete a topic.");
            System.err.println("Default topic is '"+ MEASUREMENTS_TOPIC + "' or '" + MESSAGES_TOPIC + "' according to chosen consumer/producer type.");
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

                case "producer":
                    measurementProducer(args);
                    break;

                case "consumer":
                    measurementConsumer(args);
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
        producer(topic, partition, Bootstrap::acquireTemperatureSensorMeasurement, m -> m.getDeviceId());
    }

    private static void consoleMessageProducer(Queue<String> args) {
        String topic = args.isEmpty() ? MESSAGES_TOPIC : args.remove();
        Integer partition = args.isEmpty() ? null : Integer.parseInt(args.remove());
        producer(topic, partition, consoleMessageSupplier(), m -> m.senderId);
    }

    private static void consoleMessageConsumer(Queue<String> args) {
        String topic = args.isEmpty() ? MESSAGES_TOPIC : args.remove();
        String group = args.isEmpty() ? CONSUMER_GROUP_DEFAULT : args.remove();
        consumer(topic, group, Message.class, m -> {
            System.out.println(m.senderId + ": " + m.text);
        });
    }

    private static void measurementConsumer(Queue<String> args) {
        String topic = args.isEmpty() ? MEASUREMENTS_TOPIC : args.remove();
        String group = args.isEmpty() ? CONSUMER_GROUP_DEFAULT : args.remove();
        consumer(topic, group, Measurement.class, m -> {
            System.out.println(m.toString());
        });
    }

    private static <M> void producer(String topic, Integer partition, Supplier<M> messageSupplier, Function<M, String> keyFunction) {
        LOG.info("New producer with PID " + obtainPid());
        JsonMessageProducer<M> producer = new JsonMessageProducer<>(topic, partition, kafkaProducerProps(), objectMapper(),
                messageSupplier, keyFunction, true);

        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            main.interrupt();
            try { main.join(2000); } catch (InterruptedException ie){ }
        }));

        producer.produceLoop();
    }

    private static <M> void consumer(String topic, String group, Class<M> messageType, Consumer<M> messageHandler) {
        LOG.info("New consumer with PID " + obtainPid());
        JsonMessageConsumer<M> consumer =
                new JsonMessageConsumer(topic, messageType, kafkaConsumerProps(group),
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

    /**
     * See <a href="http://kafka.apache.org/documentation.html#consumerconfigs">Consumer configs</a>
     */
    private static Map<String,Object> kafkaConsumerProps(String groupId) {
        var props = new HashMap<String,Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // In case no offset is stored, start at beginning
        return props;
    }

    /**
     * See <a href="http://kafka.apache.org/documentation.html#producerconfigs">Producer configs</a>
     */
    private static Map<String,Object> kafkaProducerProps() {
        var props = new HashMap<String,Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // value in bytes, 16384 is the default
        props.put(ProducerConfig.ACKS_CONFIG, "1");         // Require ack from leader only, default
        return props;

        // More complex setup with authentication (currently beyond scope of this demo):
//        props.put("ssl.endpoint.identification.algorithm", "https");
//        props.put("sasl.mechanism", "PLAIN");
//        props.put("request.timeout.ms", 5000);
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-43mml.europe-west2.gcp.confluent.cloud:9092");
//        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<SomeUser>\" password=\"<SomePass\>";");
//        props.put("security.protocol","SASL_SSL");
//        props.put("basic.auth.credentials.source","USER_INFO");
//        props.put("schema.registry.basic.auth.user.info", "api-key:api-secret");
//        props.put("schema.registry.url", "<schema-registry-url>");
//        props.put(ProducerConfig.CLIENT_ID_CONFIG, "no.nav.kafka.sandbox");
    }

    // Produces one synthentic temp event every 1-2 seconds
    private static Measurement acquireTemperatureSensorMeasurement() {
        // Throttling and some variance in timing
        try {
            Thread.sleep((long) (Math.random() * 1000) + 1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        final int temp = (int)(Math.random()*20 + 19);
        return new Measurement("sensor-" + obtainPid(),
                "temperature", "celcius", LocalDateTime.now(), temp);
    }

    private static Supplier<Message> consoleMessageSupplier() {
        final Console console = System.console();
        if (console == null) {
            throw new IllegalStateException("Unable to get system console");
        }
        final String senderId = "sender-" + obtainPid();
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

    private static long obtainPid() {
        return ProcessHandle.current().pid();
    }

    static ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

}
