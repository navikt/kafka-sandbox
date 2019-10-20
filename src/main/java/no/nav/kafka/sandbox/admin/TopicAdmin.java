package no.nav.kafka.sandbox.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Map;

public class TopicAdmin implements AutoCloseable {

    private final AdminClient adminClient;

    public TopicAdmin(String broker) {
        this.adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker));
    }

    /**
     * Creates topic with desired partition count and waits until it is ready before returning.
     * @param numPartitions
     */
    public void create(String topic, int numPartitions) throws Exception {
        this.adminClient.createTopics(Collections.singleton(new NewTopic(topic, numPartitions, (short)1))).all().get();
    }

    public void delete(String topic) throws Exception {
        this.adminClient.deleteTopics(Collections.singleton(topic)).all().get();
    }

    @Override
    public void close() {
        this.adminClient.close();
    }
}
