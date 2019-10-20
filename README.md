# A basic Kafka sandbox/demo app using official Java client libraries

.. and with a minimal set of dependencies.

## Purpose

- Get quickly up and running with Kafka using the standard Java Kafka clients.
- Experiment with the console clients to learn about communication patterns
  possible with Kafka, how topic partitions and consumer groups work in
  practice, and how error conditions affect the clients and the communication.
- Experiment with the settings to learn and understand behaviour.
- Easily modify and re-run code in the experimentation process.
- Contains code and examples of tests that use a local temporary Kafka
  environment to execute.


## Requirements

- [JDK 11+][1]
- [Maven][2] 3.6.X (must be able to handle modular Java project)
- A working [Docker][3] installation on localhost ([Docker for Windows][4] is fine), and
  [docker-compose][5].

[1]: https://adoptopenjdk.net/
[2]: https://maven.apache.org/download.cgi
[3]: https://www.docker.com/
[4]: https://docs.docker.com/docker-for-windows/install/
[5]: https://docs.docker.com/compose/

## Recommended reading

https://kafka.apache.org/documentation/#gettingStarted

This page explains a lot of concepts which are useful to know about beforehand.


## Getting started

### Building

The build process is boring and very standard, but does test that Docker and
docker-compose works on your host:

    $ mvn package
    
If all goes well, an executable über-jar is built in
`target/kafka-sandbox-<version>.jar`. The automated tests actually spin up Kafka
on localhost, and so take a while to complete. To skip the tests during
development iterations, use `mvn package -DskipTests` instead.

The jar-file can be executed simply by running `./run.sh` from the project
directory, or alternatively using `java -jar target/kafka-sandbox*.jar`.

### Running a Kafka environment on localhost
<a name="local-kafka"/>

Ensure the can you can get Kafka up and running on localhost. For running the
command line client of kafka-sandbox, all you need to do is run the following in
a dedicated terminal with current directory being the kafka-sandbox project
directory:

    $ docker-compose up

### Running the kafka-sandbox command line client

To get started:

    $ chmod +x run.sh
    $ ./run.sh --help
    Use: 'producer [TOPIC]' or 'consumer [TOPIC [GROUP]]'
    Use: 'console-message-producer [TOPIC]' or 'console-message-consumer [TOPIC [GROUP]]'
    Use: 'newtopic TOPIC [N]' to create a topic with N partitions (default 1).
    Use: 'deltopic TOPIC' to delete a topic.
    Default topic is 'measurements' or 'messages' according to chosen producer/consumer type.
    Default consumer group is 'console'.
    Kafka broker is expected to be running at localhost:9092

(The run script will trigger a Maven build if no JAR file exists in `target/`.)

The producer and consumer modes are paired according to the type of messages
they can exchange. The default 'producer' creates synthentic "temperature
measurement" events automatically after starting up, hence the naming of the
corresponding default Kafka topic. The default 'consumer' is able to read these
messages and display them as console output.

The 'console-message-producer' is an interactive producer that reads messages
you type on the command line and ships them off to a Kafka topic. The
'console-message-consumer' is able to read these messages and display them as
console output. These can be used to get a more controlled message production
where sending is driven by user input.

The commands 'newtopic' and 'deltopic' allows simple administration of Kafka
topics for testing purposes.


## Communication patterns with Kafka

*These examples assume that you have a local Kafka broker up and running on `localhost:9092`, 
see [relevant section](#local-kafka).*

### Example: one to one

This example is possibly the simplest case and can be easily demonstrated using
the command line clients in kafka-sandbox.

We will use the default topic with a single partition:

*In terminal 1:*

    $ ./run.sh producer

The producer will immediately start sending messages to the Kafka topic
'measurements'. Since this default topic only has one partition, the exact place
where the messages will be stored can be denoted as 'measurements-0', meaning
partition 0 for the topic.

*In terminal 2:*

    $ ./run.sh consumer
    
The consumer will connect to Kafka and starting polling for messages. It will
display the messages in the console as they arrive. The consumer subscribes to
the topic 'measurements', but does not specify any partition in particular. So
it will be assigned a partition automatically.

The consumer uses the default consumer group 'console'. The consumer group
concept is important to understand:

1. The consumer group is simply a named identifier chosen by the clients.
2. There can only be *one* consumer client instance in a particular consumer
   group assigned to a single topic-partition at any given time.
3. Consumed partition offsets for a topic is stored per *consumer group*. In
   other words, Kafka stores the progress on a *per consumer group* basis, for a
   particular topic and its partitions.
4. When a new consumer group name is established, the consumers which are part
   of that group will typically start "at the beginning" of the topic.
5. When the constellation of consumers in the same consumer group connected to a
   topic changes, Kafka will rebalance the consumers and possibly reassign
   partitions within the group.

To observe what happens when a consumer disconnects and reconnects to the same topic:

1. Stop the running consumer in terminal 2 by hitting `CTRL+C`. (You may notice
   in the Kafka broker log that the consumer instance left the topic.)
2. Start the consumer again. Notice that it does not start at the beginning of
   the Kafka topic log, but continues from the offset where it left off. This is
   because the consumer group offset is stored server side.
3. Kill the consumer again, and restart with a different (new) consumer group:

        $ ./run.sh consumer measurements othergroup
        
   Notice how it now starts displaying messages from the very beginning of the
   topic (offset 0). This is because no previous offset has been stored for the
   'othergroup' group in Kafka and the client is configured to start at the
   beginning of the topic in that situation.
   
What happens when a second consumer joins ? Start a second consumer in a new
terminal window:

        $ ./run.sh consumer measurements othergroup
        
You will now notice that one of the two running consumers will stop receiving
messages, and in that case the following message will appear:

> Rebalance: no longer assigned to topic measurements, partition 0

This is because the topic only has one partition, and only one consumer in a
single consumer group can be associated with a single topic partition at a time.

If you now kill the consumer that currently has the assignment (and shows
received messages), you will notice that Kafka does a new rebalancing, and the
previously idle consumer gets assigned back to the partition and starts
receiving messages where the other one left off.

### Example: one to many

One to many means that a single message produced on a topic is typically
processed by any number of different consumer groups.

Initialize a new topic with 1 partition and start a producer:

    $ ./run.sh newtopic one_to_many 1
    [...]
    $ ./run.sh producer one_to_many
    [...]
    
And fire up as many consumers as desired in new terminal windows, but increment
the group number N for each one:

    $ ./run.sh consumer one_to_many group-N
    
You will notice that all the consumer instances report the same messages and
offsets after a short while. Because they are all in different consumer groups,
they all see the messages that the single producer sends.

### Example: one time message processing with parallel consumer group

In this scenario, it is only desirable to process a message once, but it can be
processed by any consumer in the consumer group.

Create a topic with 3 partitions:

    $ ./run.sh newtopic any_once 3
    [...]
    
Start a producer:

    $ ./run.sh producer any_once
    
Start three consumers in the same group, run x 3:

    $ ./run.sh consumer any_once group

After starting the first one, you may see that it gets assigned to all three
partitions in the topic. After starting all three, you may notice that only one
of them actually receives and displayes messages from the producer. This is the
consumer that happens be be assigned to the partition that the single producer
is appending to.

The producer writes only to a single partition because all the measurement
messages it sends have the same "sensor device id" in the demo code, which is
used as Kafka record key. When a key is used, the partition number is chosen,
*by the producer itself*, based on a hash of the key value. So all messages
having the same key go to the same partition.

To get messages onto the other partitions of the topic, fire up some more
producers, which will be assigned different sensor-ids, and hence produce
messages with different keys:

    $ ./run.sh producer any_once
    
The producer logs the partition it writes to, so you will be able to see if any
of the new producers write to a different partition than the first one. If not,
restart some them to get assigned new ids. The producer is generally free to
choose partition and can assign a static partition to all messages if so
desired. However, the demo code in kafka-sandbox does not support that yet (feel
free to add it).

See the Apache code for class
`org.apache.kafka.clients.producer.internals.DefaultPartitioner` - it is not
complicated and explains it in detail. The partitioner class [strategy] to use
is part of the Kafka producer config.


### Many to many

The previous example can also be considered a many to many example if more
consumers are started in several active consumer groups. In that case, all the
messages produced will be handled in parallel by several different groups (but
only once per group).

### Consumer group rebalancing

You will notice log messages from the consumers whenever a consumer group
rebalancing occurs. This typically happens when a consumer leaves or a new
consumer arrives. It will provide insight into how Kafka distributes messages
amongst consumers in a group.

### Error handling in general

The demo clients in this app are "unsafe" with regard to message sending and
reception. The producer does not care about failed sends, but merely logs it as
unfortunate events. Depending on business requirements, you will likely need to
take proper care of exception handling and retry policies, to ensure no loss of
events at either the producing or consuming end.

### Error handling: broker goes down

What happens to a producer/consumer when the broker suddenly stops responding ?
In particular, what happens to the messages that are being sent ? Are they lost
or can they be accidentally reordered ?

Here is a recipe to experiment with such scenarios.

Run a producer and a consumer in two windows:

    $ ./run.sh producer
    [...]
    
    $ ./run.sh consumer
    [...]
    
Then pause the docker container with the broker to simulate that it stops
responding:

    $ docker-compose pause broker
    
Now watch the error messages from the producer that will eventually appear. A
prolonged pause will actually cause messages to be lost with the current
kafka-sandbox code. It keeps trying to send new messages without really caring
what happens to already dispatched ones. Depending on use case, this may not be
desirable, and one may need to develop code that always retries failed sends to
avoid losing events.

Make the broker respond again:

    $ docker-compose unpause broker
    
The producer recovers and sends its internal buffer of messages that have not
yet expired due to timeouts.

You may also restart the broker entirely, which causes it to lose its runtime
state, and see what happens with the clients:

    $ docker-compose restart broker
    
or:

    $ docker-compose stop broker
    $ # wait a while..
    $ docker-compose start broker

You'll notice that the clients recover eventually, but if it is down for too
long, messages will be lost. Also, you will notice rebalance notifications from
consumers once they are able to reconnect to the broker.

Behaviour can be adjusted by the many config options that the Kafka clients
support. You can experiment and modify config by editing the code in
`no.nav.kafka.sandbox.Bootstrap`, see `#kafkaProducerProps()` and
`#kafkaConsumerProps(String)`.

Useful URLs for Kafka configuration docs:

http://kafka.apache.org/documentation.html#consumerconfigs

http://kafka.apache.org/documentation.html#producerconfigs


### Error handling: consumer dies

What happens within a consumer group when an active consumer suddenly becomes
unavailable ?

Start a producer and two consumers with a simple 1 partition topic:

    $ ./run.sh producer sometopic
    
Then two consumers in other terminal windows:

    $ ./run.sh consumer sometopic group
    
You will notice that one of the consumers is idle (no "untaken" partitions in
consumer group), and the other one is assigned the active partition and is
processing messages. Figure out the PID of the *active* consumer and kill it
with `kill -9`. (The PID is printed to the console right after the consumer is
started.)

    $ kill -9 <PID>

This causes a sudden death of the consumer process and it will take a short
while until Kafka notices that the consumer is gone. Watch the broker log and
what eventually happens with the currently idle consumer.


## Tuning logging to get more details

If you would like to see the many technical details that the Kafka clients emit,
you can set the log level of the Apache Kafka clients in the file
`src/main/resources/simplelogger.properties`. It is by default `WARN`, but
`INFO` will output much more information.


## Unit/integration tests with `DockerComposeKafkaEnv`

The class `DockerComposeKafkaEnv` can be used to manage a temporary Kafka
environment for unit tests. It makes it simple to bring Kafka up/down between
tests and handles port assignments and other boring details automatically. It
also ensures that the Docker resources have unique names that should not
conflict with other containers. It requires the `docker-compose` command to
function, but has no other dependencies. Its configuration is stored in
`src/test/resources/DockerComposeKafkaEnv.yml`.

See example of usage in `KafkaSandboxTest`.

### Tips to clean up ephemeral Docker containers and networks

When developing unit tests, sometimes things go awry and the Docker containers
comprising the temporary Kafka environments are not cleaned up. Here are a few
tips to keep things tidy and free resources.

To stop and erase all `KafkaDockerComposeEnv`-created Docker containers and
networks, use the following commands:

    $ docker rm -fv $(docker ps -f name=broker-test- -f name=zookeeper-test- -q)
    $ docker network rm $(docker network ls -f name=kafkadockercomposeenv -q)


## Using kafkacat to inspect Kafka topics

When working with Kafka, a very useful command line tool is
[kafkacat](https://github.com/edenhill/kafkacat). It is a light weight, but
powerful Kafka client that supports many options.

A typical installation on Ubuntu Linux can be accomplished with:

    $ sudo apt install kafkacat


## Using official Kafka command line tools

You can connect to the Docker container running the Kafka broker and get access
to some interesting command line tools:

    $ docker exec -it broker /bin/bash -i
    
Then type "kafka-" and press TAB a couple of times to find the commands
available.
