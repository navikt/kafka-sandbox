package no.nav.kafka.sandbox.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopicAdmin implements AutoCloseable {

    private final AdminClient adminClient;

    public TopicAdmin(String broker) {
        adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker));
    }

    /**
     * Creates topic with desired partition count and waits until it is ready before returning.
     * @param numPartitions
     */
    public void create(String topic, int numPartitions) throws Exception {
        adminClient.createTopics(Collections.singleton(new NewTopic(topic, numPartitions, (short)1))).all().get();
    }

    public void delete(String topic) throws Exception {
        adminClient.deleteTopics(Collections.singleton(topic)).all().get();
    }

    public List<String> listTopics() throws Exception {
        return adminClient.describeTopics(adminClient.listTopics().names().get()).all().get()
                .values().stream().flatMap(
                        td -> td.partitions().stream().map(p -> td.name() + "-" + p.partition()))
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        this.adminClient.close();
    }
}
