package no.nav.kafka.sandbox;

import no.nav.kafka.sandbox.messages.ConsoleMessages.Message;
import no.nav.kafka.sandbox.consumer.JsonMessageConsumer;
import no.nav.kafka.sandbox.producer.JsonMessageProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Demonstrates use of {@link DockerComposeEnv}, and a few simple tests using a running Kafka instance.
 */
public class KafkaSandboxTest {

    private static DockerComposeEnv dockerComposeEnv;

    private static AdminClient adminClient;

    private static Logger LOG = LoggerFactory.getLogger(KafkaSandboxTest.class);

    private final String testTopic;

    public KafkaSandboxTest() {
        testTopic = nextTestTopic();
    }

    private static final AtomicInteger testTopicCounter = new AtomicInteger(0);
    private static String nextTestTopic() {
        return "test-topic-" + testTopicCounter.incrementAndGet();
    }

    @BeforeAll
    public static void dockerComposeUp() throws Exception {
        Assumptions.assumeTrue(DockerComposeEnv.dockerComposeAvailable(),
                "This test needs a working 'docker-compose' command");

        dockerComposeEnv = DockerComposeEnv.builder("src/test/resources/KafkaDockerComposeEnv.yml")
                .addAutoPortVariable("KAFKA_PORT")
                .dockerComposeLogDir("target/")
                .readyWhenPortIsOpen("localhost", "KAFKA_PORT")
                .up();

        adminClient = newAdminClient();
    }

    @AfterAll
    public static void dockerComposeDown() throws Exception {
        if (dockerComposeEnv != null) {
            adminClient.close();
            dockerComposeEnv.down();
        }
    }

    static int kafkaPort() {
        return Integer.parseInt(dockerComposeEnv.getEnvVariable("KAFKA_PORT"));
    }

    @BeforeEach
    public void createTestTopic() throws Exception {
        adminClient.createTopics(Collections.singleton(new NewTopic(testTopic, 1, (short)1))).all().get();
    }

    @AfterEach
    public void deleteTestTopic() throws Exception {
        adminClient.deleteTopics(Collections.singleton(testTopic)).all().get();
    }

    @Test
    public void waitForMessagesBeforeSending() throws Exception {
        LOG.debug("waitForMessagesBeforeSending start");
        final List<String> messages = List.of("one", "two", "three", "four");

        final CountDownLatch waitForAllMessages = new CountDownLatch(messages.size());

        Executors.newSingleThreadExecutor().execute(() -> {
            try (KafkaConsumer<String,String> consumer = newConsumer("test-group")) {
                consumer.subscribe(Collections.singleton(testTopic));
                int pollCount = 0;
                while (waitForAllMessages.getCount() > 0 && pollCount++ < 5) {
                    consumer.poll(Duration.ofSeconds(10)).forEach(record -> {
                        LOG.debug("Received message: " + record.value());
                        waitForAllMessages.countDown();
                    });
                }
            }
        });

        // Consumer is now ready and waiting for data, produce something without waiting for acks
        try (KafkaProducer<String, String> producer = newProducer()) {
            messages.forEach(val -> {
                LOG.debug("Sending message: " + val);
                producer.send(new ProducerRecord<>(testTopic, val));
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ie) {
                }
            });
        }

        // Wait for consumer to receive the expected number of messages from Kafka
        assertTrue(waitForAllMessages.await(30, TimeUnit.SECONDS), "Did not receive expected number of messages within time frame");
    }

    @Test
    public void sendThenReceiveMessages() throws Exception {
        LOG.debug("sendThenReceiveMessage start");
        final List<String> messages = List.of("one", "two", "three", "four");

        final CountDownLatch productionFinished = new CountDownLatch(messages.size());
        try (KafkaProducer<String, String> producer = newProducer()) {
            messages.forEach(val -> {
                producer.send(new ProducerRecord<>(testTopic, val), (metadata, exception) -> {
                    if (exception == null) {
                        productionFinished.countDown();
                    }
                });
            });
            productionFinished.await(30, TimeUnit.SECONDS); // Wait for shipment of messages
        }

        final List<String> received = new ArrayList<>();
        try (KafkaConsumer<String,String> consumer = newConsumer("test-group")) {
            consumer.subscribe(Collections.singleton(testTopic));
            int pollCount = 0;
            while (received.size() < messages.size() && pollCount++ < 10) {
                consumer.poll(Duration.ofSeconds(1)).forEach(cr -> {
                    received.add(cr.value());
                });
            }
        }

        LOG.debug("Received messages: {}", received);

        assertEquals(messages.size(), received.size(), "Number of messages received not equal to number of messages sent");
    }

    @Test
    public void testJsonMessageConsumerAndProducer() throws Exception {

        final BlockingQueue<Message> outbox = new ArrayBlockingQueue<>(1);
        final Supplier<Message> supplier = () -> {
            try { return outbox.take(); } catch (InterruptedException ie) {
                throw new InterruptException(ie);
            }
        };

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        final var producer = new JsonMessageProducer<Message>(testTopic, null, kafkaProducerTestConfig(kafkaPort()),
                Bootstrap.objectMapper(), supplier , m -> null, true);
        final Future<?> producerLoop = executor.submit(producer::produceLoop);

        outbox.offer(new Message("Hello", "test-sender"));

        // ---- now fetch message using consumer ----

        final BlockingQueue<Message> inbox = new ArrayBlockingQueue<>(1);
        final var consumer = new JsonMessageConsumer<Message>(testTopic,
                Message.class,
                kafkaConsumerTestConfig(kafkaPort(), "testGroup"),
                Bootstrap.objectMapper(), m -> inbox.offer(m) );
        final Future<?> consumeLoop = executor.submit(consumer::consumeLoop);

        Message success = inbox.take();
        assertEquals("Hello", success.text);
        assertNull(inbox.poll());

        producerLoop.cancel(true);
        consumeLoop.cancel(true);
        executor.shutdown();
    }

    private static AdminClient newAdminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:"+ kafkaPort()));
    }

    private static KafkaConsumer<String,String> newConsumer(String group) {
        return new KafkaConsumer<>(kafkaConsumerTestConfig(kafkaPort(), group));
    }

    private static KafkaProducer<String,String> newProducer() {
        return new KafkaProducer<>(kafkaProducerTestConfig(kafkaPort()));
    }

    private static Map<String,Object> kafkaConsumerTestConfig(int kafkaPort, String consumerGroup) {
        return Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroup,
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }

    private static Map<String,Object> kafkaProducerTestConfig(int kafkaPort) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );
    }

}
