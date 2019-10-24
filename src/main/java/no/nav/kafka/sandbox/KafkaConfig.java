package no.nav.kafka.sandbox;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Common Kafka config for producer/consumers.
 */
class KafkaConfig {

    /**
     * See <a href="http://kafka.apache.org/documentation.html#producerconfigs">Producer configs</a>
     */
    static Map<String,Object> kafkaProducerProps() {
        var props = new HashMap<String,Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.DEFAULT_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // default is "infinite" number of retries, within the constraints of other related settings
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000); // 10 seconds delivery timeout, default is 120 seconds
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);  // Request timeout, default is 30 seconds
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);               // At default value of 0, affects batching of messages
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);          // at default value of 16384 bytes
        props.put(ProducerConfig.ACKS_CONFIG, "1");                  // Require ack from leader only, at default value
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

    /**
     * See <a href="http://kafka.apache.org/documentation.html#consumerconfigs">Consumer configs</a>
     */
    static Map<String,Object> kafkaConsumerProps(String groupId) {
        var props = new HashMap<String,Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.DEFAULT_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // In case no offset is stored for consumer group, start at beginning
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);  // Max time allowed for message handling between calls to poll, default value
        return props;
    }
}
