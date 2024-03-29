# Kafka sandbox - experiments with Kafka clients and Spring Kafka

- Kafka Java client 3.6, Java 17+
- Spring Kafka 3.1, Spring Boot 3.2 (optional in separate module)
- Basic plain Java producer/consumer clients with minimal dependencies.
- A Spring Boot application with Kafka consumer endpoints, internal storage and
  web interfaces.
- Batch error handling with Spring Kafka.
- Learn by doing, observing and experimenting.

## Purpose

- Get quickly up and running with Kafka using the standard Java Kafka clients.
- Experiment with the console clients to learn about communication patterns
  possible with Kafka, how topic partitions and consumer groups work in
  practice, and how error conditions affect the clients and the communication.
- Experiment with the settings to learn and understand behaviour, easily modify
  and re-run code in the experimentation process.
- Learn and experiment with setup of multiple Kafka consumers in a Spring Boot
  application.
- Learn about batch error handling strategies in Spring Kafka.
- Contains code and examples of tests that use a local temporary Kafka
  environment to execute.
- Even though not the primary purpose of this project: Learn about Java module
  system and modular Maven builds.
  
It is important to note that this is not a complete guide to Kafka or Spring
Kafka in any sense. It can however serve as a base setup (or "sandbox") which
will enable you to quickly experiment with Kafka clients and Spring Kafka.
Things can get really complicated, and sometimes you need to actually test how
things work in a small and controlled environment, to understand the technology
and apply it correctly in real production scenarios.

## Requirements

- [JDK 17+][1]
- [Maven][2] 3.8.X+ (must be able to handle modular Java project and modern
  Java compiler options)
- A working [Docker][3] installation on localhost ([Docker for Windows][4] is
  fine), and [docker compose][5].
- A unix-like shell is very handy, but not a strict requirement.

[1]: https://adoptium.net/temurin/releases/
[2]: https://maven.apache.org/
[3]: https://www.docker.com/
[4]: https://docs.docker.com/docker-for-windows/install/
[5]: https://docs.docker.com/compose/

## Recommended reading

https://kafka.apache.org/documentation/#gettingStarted

This page explains a lot of concepts which are useful to know about beforehand.

And if interested in Spring Kafka:
https://docs.spring.io/spring-kafka/reference/index.html

## Index

1. [Getting started](#getting-started)
2. [Communication patterns with Kafka](#kafka-patterns)
   1. [One to one](#kafka-one-to-one)
   2. [One to many](#kafka-one-to-many)
   3. [One time processing with parallel consumer group](#kafka-one-time-parallel)
   4. [Many to many](#kafka-many-to-many)
   5. [Consumer group rebalancing](#kafka-consumer-group-rebalancing)
   6. [Many to one](#kafka-many-to-one)
   7. [Error handling: broker goes down](#kafka-error-broker-down)
   8. [Error handling: detecting message loss](#kafka-message-loss)
   9. [Error handling: consumer dies](#kafka-consumer-dies)
3. [The Spring Boot application](#spring-boot)
   1. [Running](#spring-running)
   2. [Web interfaces](#spring-web-interfaces)
   3. [Talk to Spring boot application (or yourself) using Kafka](#spring-talk-to)
   4. [Experiment with Spring application](#spring-experiment)
   5. [Batch consumer error handling in Spring Kafka](#spring-batch-error-1)
   6. [Batch consumer error handling in Spring Kafka: infinite retries](#spring-batch-error-2)
   7. [Batch consumer error handling in Spring Kafka: limited retries](#spring-batch-error-4)
   8. [Batch consumer error handling in Spring Kafka: really limited retries](#spring-batch-error-4)
   9. [Batch consumer error handling in Spring Kafka: recovery](#spring-batch-error-5)
   10. [What about transient failures when storing events ?](#spring-batch-error-6)
   11. [Handling failure to deserialize messages in batch consumer](#spring-batch-error-7)
   12. [Stopping consumer on fatal errors](#spring-batch-error-8)
   
4. [Tuning logging to get more details](#log-tuning)
5. [Unit/integration tests with `DockerComposeEnv`](#integration-tests)
6. [Using kafkacat to inspect Kafka topics](#kafkacat)
7. [Using official Kafka command line tools](#kafka-cmds)

## Getting started                                       <a name="getting-started"/>

### Building

The project consists of three Maven modules:

1. messages
2. clients
3. clients-spring

The `messages` module is used by both the Spring application and regular command
line clients and contains various messages types and handling of them.

The build process is boring and very standard, but does test that Docker and
docker-compose works on your host:

    mvn install
    
If all goes well, an executable jar is built in
`clients/target/clients-<version>.jar` for the basic Java clients. The automated
tests actually spin up Kafka on localhost, and so take a while to complete. To
skip the tests during development iterations, use `mvn install -DskipTests`
instead.

The jar-file can be executed simply by running `./run` from the project
top directory, or alternatively using `java -jar clients/target/clients-*.jar`.

### Running a Kafka environment on localhost             <a name="local-kafka"/>

Ensure the can you can get Kafka up and running on localhost. For running the
command line clients or Spring boot application of kafka-sandbox, all you need
to do is run the following in a dedicated terminal with current directory being
the kafka-sandbox project directory:

    docker compose up -d && docker compose logs -f

To shut down the Kafka service containers, press `CTRL+C` to exit the logs view
and then type:

    docker compose down

### Running the kafka-sandbox command line clients

To get started:

    $ chmod +x run
    $ ./run --help
    Use: 'producer [TOPIC [P]]' or 'consumer [TOPIC [GROUP]]'
    Use: 'produce N [TOPIC [P]]' to produce exactly N measurements to topic with no delay
    Use: 'console-message-producer [TOPIC [P]]' or 'console-message-consumer [TOPIC [GROUP]]'
    Use: 'sequence-producer [TOPIC [P]]' or 'sequence-consumer [TOPIC [GROUP]]'
    Use: 'null-producer [TOPIC [P]]' to produce a single message with null value
    Use: 'string-producer STRING [TOPIC [P]]' to produce a single UTF-8 encoded string message
    Use: 'newtopic TOPIC [N]' to create a topic with N partitions (default 1).
    Use: 'deltopic TOPIC' to delete a topic.
    Use: 'showtopics' to list topics/partitions available.
    Default topic is chosen according to consumer/producer type.
    Default consumer group is 'console'
    Kafka broker is localhost:9092

*Note: after code changes, you should rebuild the project with `mvn install` so
that executable jars are updated. The convenience run scripts will not
automatically perform rebuilds.*

The producer and consumer modes are paired according to the type of messages
they can exchange. The default 'producer' creates synthentic "temperature
measurement" events automatically after starting up, hence the naming of the
corresponding default Kafka topic. The default 'consumer' is able to read these
messages and display them as console output.

The 'sequence-producer' creates records with an ever increasing sequence number.
The corresponding consumer does a simple validation of received messages,
checking that the received sequence number is the expected one. This can be used
to detect if messages are lost or reordered in various situations. The consumer
keeps an account of the number of errors detected and writes status to stdout
upon message reception.

The 'console-message-producer' is an interactive producer that reads messages
you type on the command line and ships them off to a Kafka topic. The
'console-message-consumer' is able to read these messages and display them as
console output. These can be used to get a more controlled message production
where sending is driven by user input.

The 'null-producer' produces a single record with a null value to a topic. Used
for testing error handling of poison pill messages. The 'string-producer' can
produce records with UTF-8 encoded strings as payload. It can be used to trigger
deserialization errors for consumers expecting JSON payload.

The commands 'newtopic', 'deltopic' and 'showtopics' allow simple administration
of Kafka topics for testing purposes.

### Tip regarding terminal usage

The examples generally require multiple terminal windows, in order to start
different producer/consumer processes in parallel with visible output. Using
only terminal tabs is perhaps not the best option, because you will not be able
to see in real time the different outputs together. If you are already using
something like tmux you can benefit greatly by using multiple panes and windows.

### Running directly from IntelliJ

You can create run configurations in IntelliJ for all the examples, by starting
the `no.nav.kafka.sandbox.Bootstrap` class with the arguments and create a
Spring boot run configuration for the `Application` class in `clients-spring`.
So a shell is not strictly required.

## Communication patterns with Kafka                 <a name="kafka-patterns"/>

*These examples assume that you have a local Kafka broker up and running on `localhost:9092`, 
see [relevant section](#local-kafka).*

### Example: one to one                              <a name="kafka-one-to-one"/>

This example is possibly the simplest case and can be easily demonstrated using
the command line clients in kafka-sandbox.

We will use the default topic with a single partition:

*In terminal 1:*

    ./run producer

The producer will immediately start sending messages to the Kafka topic
'measurements'. Since this default topic only has one partition, the exact place
where the messages will be stored can be denoted as 'measurements-0', meaning
partition 0 for the topic.

*In terminal 2:*

    ./run consumer
    
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
   particular topic and its partitions. (Consumer clients are responsible for
   comitting the progress back to Kafka.)
4. When a new consumer group name is established, the consumers which are part
   of that group will typically start receiving only new messages sent to the
   topic. This is however configurable, and the consumers in kafka-sandbox are
   by default setup to start at the very beginning of a topic, if Kafka has no
   stored offset data for the group to begin with.
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

        ./run consumer measurements othergroup
        
   Notice how it now starts displaying messages from the very beginning of the
   topic (offset 0). This is because no previous offset has been stored for the
   'othergroup' group in Kafka and the client is configured to start at the
   beginning of the topic in that situation.
   
What happens when a second consumer joins ? Start a second consumer in a new
terminal window:

    ./run consumer measurements othergroup
        
You will now notice that one of the two running consumers will stop receiving
messages, and in that case the following message will appear:

> Rebalance: no longer assigned to topic measurements, partition 0

This is because the topic only has one partition, and only one consumer in a
single consumer group can be associated with a single topic partition at a time.

If you now kill the consumer that currently has the assignment (and shows
received messages), you will notice that Kafka does a new rebalancing, and the
previously idle consumer gets assigned back to the partition and starts
receiving messages where the other one left off.

### Example: one to many                         <a name="kafka-one-to-many"/>

One to many means that a single message produced on a topic is typically
processed by any number of different consumer groups.

Initialize a new topic with 1 partition and start a producer:

    ./run newtopic one_to_many 1

    ./run producer one_to_many

    
And fire up as many consumers as desired in new terminal windows, but increment
the group number N for each one:

    ./run consumer one_to_many group-N
    
You will notice that all the consumer instances report the same messages and
offsets after a short while. Because they are all in different consumer groups,
they all see the messages that the single producer sends.

### Example: one time message processing with parallel consumer group       <a name="kafka-one-time-parallel"/>

In this scenario, it is only desirable to process a message once, but it can be
processed by any consumer in a consumer group.

Create a topic with 3 partitions:

    ./run newtopic any_once 3
    
Start three producers in three terminals, one for each partition:

    ./run producer any_once 0

    ./run producer any_once 1

    ./run producer any_once 2

Here we are explicitly specifying which partition each producer should write to,
so that we ensure an even distribution of messages for the purpose of this
example. If partition is left unspecified, the producer will select a partition
based on the Kafka record keys. The producer of "measurement" messages in the
demo code uses a fixed "sensor device id" based on the PID as key, and so the
messages become fixed to a random partition. See the Apache code for class
`org.apache.kafka.clients.producer.internals.DefaultPartitioner` - it is not
complicated and explains it in detail. The partitioner class [strategy] to use
is part of the Kafka producer config.

Next, we are going to start consumer processes.

Begin with a single consumer:

    ./run consumer any_once group
    
You will notice that this first consumer gets assigned all three partitions on
the topic and starts displaying received messages.

Let's scale up to another consumer. Run in a new terminal:

    ./run consumer any_once group

When this consumer joins, you can see rebalancing messages, and it will be
assigned one or two partitions from the topic, while the first is removed from
the corresponding number of partitions. Now the load is divided betweeen the two
running consumers.

Scale further by starting a third consumer in a new terminal:

    ./run consumer any_once group

After the third one joins, a new rebalancing will occur and they will each have
one partition assigned. Now the load is divided evenly and messages are
processed by three parallel processes.

Try to start another fourth consumer (same topic/group) and see what happens.
(Hint: you will not gain anything wrt. message processing capacity.)


### Many to many                                         <a name="kafka-many-to-many"/>

The previous example can also be considered a many to many example if more
consumers are started in several active consumer groups. In that case, all the
messages produced will be handled in parallel by several different groups (but
only once per group).


### Consumer group rebalancing                         <a name="kafka-consumer-group-rebalancing"/>

You will notice log messages from the consumers whenever a consumer group
rebalancing occurs. This typically happens when a consumer leaves or a new
consumer arrives. It will provide insight into how Kafka distributes messages
amongst consumers in a group.


### Many to one                                       <a name="kafka-many-to-one"/>

This example demonstrates a many-to-one case, where there are lots of producers
collecting "temperature sensor events" and sending it to a common topic, while a
single consumer is responsible for processing the messages.

Start a single consumer for topic 'manydevices':

    ./run consumer manydevices

Start 10 producers by executing the following command 10 times:

    $ ./run producer manydevices 1>/dev/null 2>&1 &
    [x10..]
    
The producers will be started in the background by the shell and the output is
hidden. (You can examine running background jobs with the command `jobs`.)

After a short while, you should see the consumer receiving and processing
messages from many different producers ("sensor devices"). Depending on the
number of running producers, you may see the consumer receiving multiple records
per poll call to Kafka. This is simply due to the increased rate of messages
being written to the topic.

To kill all producers running in the background, execute command:

    kill $(jobs -p)


### Error handling in general

The demo clients in this app are "unsafe" with regard to message sending and
reception. The producer does not care about failed sends, but merely logs it as
unfortunate events. Depending on business requirements, you will likely need to
take proper care of exception handling and retry policies, to ensure no loss of
events at either the producing or consuming end.

### Error handling: broker goes down                   <a name="kafka-error-broker-down"/>

What happens to a producer/consumer when the broker suddenly stops responding ?
In particular, what happens to the messages that are being sent ? Are they lost
or can they be accidentally reordered ?

Here is a recipe to experiment with such scenarios.

Run a producer and a consumer in two windows:

    $ ./run producer
    [...]
    
    $ ./run consumer
    [...]
    
Then pause the docker container with the broker to simulate that it stops
responding:

    docker compose pause broker
    
Now watch the error messages from the producer that will eventually appear. A
prolonged pause will actually cause messages to be lost with the current
kafka-sandbox code. It keeps trying to send new messages without really caring
what happens to already dispatched ones. Depending on use case, this may not be
desirable, and one may need to develop code that always retries failed sends to
avoid losing events.

Make the broker respond again:

    docker compose unpause broker
    
The producer recovers and sends its internal buffer of messages that have not
yet expired due to timeouts.

You may also restart the broker entirely, which causes it to lose its runtime
state, and see what happens with the clients:

    docker compose restart broker
    
or:

    docker-compose stop broker
    # wait a while..
    docker-compose start broker

You'll notice that the clients recover eventually, but if it is down for too
long, messages will be lost. Also, you will notice rebalance notifications from
consumers once they are able to reconnect to the broker.

Behaviour can be adjusted by the many config options that the Kafka clients
support. You can experiment and modify config by editing the code in
`no.nav.kafka.sandbox.KafkaConfig`, see `#kafkaProducerProps()` and
`#kafkaConsumerProps(String)`.

Useful URLs for Kafka configuration docs:

http://kafka.apache.org/documentation.html#consumerconfigs

http://kafka.apache.org/documentation.html#producerconfigs

### Error handling: detecting message loss with sequence-producer/consumer   <a name="kafka-message-loss"/>

The 'sequence-producer' and corresponding 'sequence-consumer' commands can be
used for simple detection of message loss or reordering. The producer will send
messages containing an ever increasing sequence number, and the consumer
validates that the messages it receives have the expected next number in the
sequence. When validation fails it logs errors and increases an error counter,
so that it is easy to spot.

Start the producer:

    ./run sequence-producer
    
It will start at sequence number 0. If you restart, it will continue from where
was last stopped, since the next sequence number is persisted to a temporary
file. (To reset this, stop the sequence-producer and remove the file
`sequence-producer.state`.)

Now start the corresponding consumer:

    ./run sequence-consumer
    
It will read the sequence numbers already on the topic and log its state upon
every message reception. You should see that the sequence is "in sync" and that
the error count is 0.

While they are running, restart the Kafka broker:

    docker compose restart broker
    
You should see the producer keeps sending messages, but does not receive
acknowledgements. Eventually it will log errors about expired messages. The
consumer may also start logging errors about connectivity, depending on how long
the broker is down, which depends on how fast the host machine is. (If the
broker restart is too quick to cause any errors, use "stop/start" instead, and
wait a little while before starting.)

Normally, with the current code in kafka-sandbox, you can observe that some
messages are lost in this process, and the consumer increases the error count
due to receiving an unexpected sequence number.

There is a challenge here: modify the kafka-sandbox code or config make it more
resilient. Ensure that no sequence messages are lost if Kafka stops responding
for about 60 seconds, for whatever reason. Test by re-running the procedure
described in this section. (Hint: see the various producer timeout config
parameters.)

To only display output related to the sequence number producer/consumer, you can
pipe the output of the start commands to `...|grep SEQ`, which will filter out
the other log messages.


### Error handling: consumer dies                       <a name="kafka-consumer-dies"/>

What happens within a consumer group when an active consumer suddenly becomes
unavailable ?

Start a producer and two consumers with a simple 1 partition topic:

    ./run producer sometopic
    
Then two consumers in other terminal windows:

    ./run consumer sometopic group
    
You will notice that one of the consumers is idle (no "untaken" partitions in
consumer group), and the other one is assigned the active partition and is
processing messages. Figure out the PID of the *active* consumer and kill it
with `kill -9`. (The PID is printed to the console right after the consumer is
started.)

    kill -9 <PID>

This causes a sudden death of the consumer process and it will take a short
while until Kafka notices that the consumer is gone. Watch the broker log and
what eventually happens with the currently idle consumer. (Note that it may take
up to a minute before Kafka decides to hand the partition over to the idle, but
live consumer.)

## The Spring Boot application                       <a name="spring-boot"/>

The Spring Boot application is implemented in Maven module `clients-spring/`.

*The application requires that you have a local Kafka broker up and running on
`localhost:9092`, see [relevant section](#local-kafka).*

### Running                                          <a name="spring-running"/>

From the top level directory:

    mvn install
    java -jar clients-spring/target/clients-spring-*.jar

.. or use the convenience script `boot-app`, which is used in all examples:

    chmod +x boot-app
    ./boot-app
    
*Note: after code changes, you should rebuild the project with `mvn install` so
that executable jars are updated. The convenience run scripts will not
automatically perform rebuilds.*

The application will automatically subscribe to and start consuming messages
from the topics `measurements` (the standard producer in previous examples) and
`messages` (for messages created by `console-message-producer` client). The
consumed messages are stored in-memory in a fixed size event store that also
detects duplicates.

### Web interfaces                             <a name="spring-web-interfaces"/>

A welcome page links to the Measurements and Messages pages of the application.
Navigate to http://localhost:8080/ to open it.

#### Measurements page

http://localhost:8080/measurements.html

A web page showing measurements/"sensor event" messages from Kafka. It uses an
API endpoint available at http://localhost:8080/measurements/api
 
#### Messages page

http://localhost:8080/messages.html

A web page showing "console message" events from Kafka. It uses an API endpoint
available at http://localhost:8080/messages/api


### Talk to Spring boot app (or yourself) using Kafka    <a name="spring-talk-to"/>

1. In one terminal, start the Spring boot application as described earlier.

2. In another terminal, from project root dir, start the command line console
   message producer:

        $ ./run console-message-producer
        36 [main] INFO Bootstrap - New producer with PID 19445
        191 [main] INFO JsonMessageProducer - Start producer loop
        Send messages to Kafka, use CTRL+D to exit gracefully.
        Type message>

3. Navigate your web browser to http://localhost:8080/messages.html 

4. Type a message into the terminal. As soon as the Spring application has
   consumed the message from Kafka, the web page will display it.
   
### Show live view of measurements as they are consumed by Spring application

1. In one terminal, start the Spring boot application as described earlier.

2. In another terminal, start a measurement producer: 

        ./run producer

3. Navigate your web browser to http://localhost:8080/measurements.html

4. Observe live as new measurement events are consumed by the Spring application
   and displayed on the page. New events are highlighted for a brief period to
   make them visually easier to distinguish.

### Experiment with Spring application            <a name="spring-experiment"/>

In the previous scenario, try to artificically slow down the Spring application
consumer and see what happens to the size of the batches that it consumes. To
slow it down, start with the following arguments:

    ./boot-app --measurements.consumer.slowdown=6000

This will make the Kakfa listener endpoint in
`no.nav.kafka.sandbox.measurements.MeasurementsConsumer#receive` halt for 6
seconds every time Spring invokes the method with incoming messages. This
endpoint is setup with batching enabled, so you should see larger batches being
processed, depending on amount of messages produced, and how slow the consumer
is.

The listener endpoint logs the size of batches it processes, so you will see it
in the application log. By default, the listener endpoint is invoked by at most
two Spring-kafka managed threads (each with their own `KafkaConsumer`). This is
setup in `no.nav.kafka.sandbox.measurements.MeasurementsConfig`, locate line
with `factory.setConcurrency(2);`. Do you think concurrency above 1 has any
effect when the topic only has one partition `measurements-0` ? Inspect the
thread-ids in the application log as it consumes messages, to determine if there
is actually more than one thread invoking the listener method.

To produce more messages in parallel, you can start more producers in the
background:

    ./run producer &>/dev/null &
        
As more producers start, you should notice the logged batch sizes increase,
since volume of messages increases and the consumer is slowed down. (Note: to
clean up producers running in the background, you can kill them with `kill
$(jobs -p)`.)

You could also produce 1000 messages with no delay, to really see batch size
increase on the consumer side:

    ./run produce 1000

Going further, you can test true parallel messages consumption in Spring, by
changing the number of partitions on the `measurements` topic:

1. Stop Spring Boot application and any running command line producer/consumer
   clients.

2. Delete measurements topic: `./run deltopic measurements`. 

3. Create new measurements topic with 4 partitions: `./run newtopic measurements 4`

4. Start Spring boot application as described earlier.

5. Start several producers in the background, as described earlier.

6. Watch Spring application log and notice that there are now two different
   thread ids invoking the listener endpoint in
   `no.nav.kafka.sandbox.measurements.MeasurementsConsumer#receive`.
   

### Experiment: default batch consumer error handling in Spring Kafka        <a name="spring-batch-error-1"/>

_For the following experiments, you should ensure the Spring Boot app is
*stopped* before following the instructions._

#### Thoughts about error handling 

Error handling is important in a distributed asynchronous world. It can be
difficult to get right, both because the various error situations can be
complex, but also hard to reproduce or picture in advance. The typical result of
poor or ignored error handling is growing inconsistencies between data in
various systems. In others words, things will _eventually_ become _inconsistent_
intead of _consistent_ !

When you start tackling error handling, you should first have a very clear
picture of the business requirements for your application.

- Is it acceptable to process a single record multiple times if batch errors occur ?
- Is it acceptable to lose records entirely ?
- Is it necessary to preserve failed records, so that proper processing can be
  done at a later time (dead letter topic) ?
- Is ordering important so that events happening after an error has occured
  (later offsets produced on a Kafka topic) must all wait to be processed until
  the error has been resolved ?

There are several strategies that can be applied to error handling, and this
guide only covers a small part of it, namely the core error handlers used by
Spring Kafka consumers. Batch error handling is more difficult to get right than
single message error handling, since errors typically affect all messages in a
batch, regardless if just some of them are the cause of the failure.

Spring Kafka has sophisticted support for various error handling strategies and
one can easily become lost in options and configuration. Start with the simplest
possible strategy, and keep it as simple as possible at all times, when
implementing your business requirements.

Topics such as outbox pattern, integrating Kafka with transactions and dead
letter topics are not covered by this guide.

#### On to experiment

You can simulate failure to store events by adjusting the configuration property
`measurements.event-store.failure-rate`. It can either be a floating point
number between 0 and 1 that determines how often the store that the consumer
saves events to should fail, randomly distributed. Or it can be specified as
`F/T`, where `F` is the number of failures that should occur in a series, and
`T` is the total number of stores before the this pattern repeats. A failed
store will cause an exception to be thrown which will trigger Spring kafka
consumer error handling.

You can also simulate bad messages by using `./run null-producer` or
`./run string-producer "{bad-json"` (will cause deserialization failure
in Spring Boot app).


Try this:

1. Ensure a producer for 'measurements' topic is running:

        ./run producer
        
2. Start Spring Boot app in another terminal with:

        ./boot-app --measurements.event-store.failure-rate=1
        
Now all (100%) store operations will fail with `IOException`. See what happens
in the application log. Error handling is all Spring Kafka defaults, which means
that no particular error handler has been configured and the
`FallbackBatchErrorHandler` will be used automatically by Spring. Does Spring
commit offsets when exceptions occur in consumer ? In other words, does it
progress beyond the point of failure and discard the failed records ?

As we can see in the application log, for each failed record the consumer is
invoked 10 times, then an ERROR is logged by Spring, telling us that the
record(s) is discarded. So the default error handler retries 10 times before
giving up. Then offsets are committed and so it progresses, and the record is
lost.

Let's try to determine the error resilience of the consumer using the default
Spring error handler and no recovery options.

1. Ensure all producers and Spring Boot app are stopped. Clear measurements
   topic and produce exactly 100 records:

        ./run deltopic measurements
        ./run produce 100

2. Then start Spring Boot app and set event store failure rate to 5/10, meaning 5
   errors, then 5 successful stores, and so on:

        ./boot-app --measurements.event-store.failure-rate=5/10

In the app logs you will notice that the consumer receives all 100 records per
batch attempt, which is logged by `MeasurementsConsumer`. This happens multiple
times until a message from the error handler logs that all 100 Kafka records
have been discarded. Navigate to http://localhost:8080/measurements.html and
check how many events where succesfully stored. How come there are exactly 5
events that have been successfully stored ? Also, does Spring by default delay
attempts to redeliver failed batches ?

#### Explanation

1. Spring Kafka retries failed batches a limited number of times by default
   (with no delay). The first 5 attempts will all fail immediately, since this
   is how we have configured the event store to fail.
2. On the sixth attempt, the 5 first events from the batch will be stored
   successfully, then this batch will also fail.
3. On the last remaining 4 attempts, the batches will immediately fail on first
   store attempt again.
4. This gives a total of 5 successfully stored events.


### Logging and ignoring all errors

You can use an error handler which ignores errors, does no retries, but still
logs them:

    ./boot-app --measurements.event-store.failure-rate=1 --measurements.consumer.error-handler=log-and-ignore

Start a producer and watch Spring Boot log. You'll see that batches are logged
as errors, but never retried. Spring Kafka progresses; it logs the failed
batches, skips them and commits offset.


### Experiment: batch consumer error handling in Spring Kafka: infinite retries     <a name="spring-batch-error-2"/>

Try this:

1. Start Spring Boot app with infinite retry error handler:

        ./boot-app --measurements.consumer.error-handler=infinite-retry

2. In another terminal, send a single null-message to the "measurements" topic:

        ./run null-producer measurements

This message will fail in the consumer with a `NullPointerException`, since
messages with a `null` value are not accepted by the consumer (although they are
allowed by Kafka, and do not fail on JSON deserialization).

Does Spring ever give up retrying the failing batch ? Watch the log from the
Spring Boot app. It continues to try forever without really logging much. When
quitting the app by pressing `CTRL+C`, Spring logs en ERROR that it was stopped
while retry error handling was in progress.


### Experiment: batch consumer error handling in Spring Kafka: limited retries    <a name="spring-batch-error-4"/>

To control number of retries for failed messages when using batch consumer, you
will typically configure a certain `BackOff` spec on the `DefaultErrorHandler`.
This controls how many times processin of a batch will be retried and how long
it should pause/backoff between attempts.

Try this:

1. Clear topic measurements with `./run deltopic measurements`.

2. Produce 3 measurements to the topic:

        ./run produce 3
        
3. Send a poison pill `null` message to the measurements topic:

        ./run null-producer measurements
        
4. Send 3 more valid measurement events:

        ./run produce 3
        
    After quitting there will be 7 messages present on the topic, including the
    `null` message in the middle.

5. Start Spring Boot app with retrying error handler that should give up after 2 retries:

        ./boot-app --measurements.consumer.error-handler=retry-with-backoff

Watch the logs. The batch is processed multiple times by `MeasurementsConsumer`,
however it gives up after two retries, since the `null` message will cause a
failure in the middle of the batch processing. Note that only the events
processed before the poison pill in the batch are actually stored. We lost the
messages coming after the poison `null` message in the batch entirely ! See if
you can spot the end result by navigating to
[http://localhost:8080/measurements.html](http://localhost:8080/measurements.html).
You should only see the messages produced in step 2. (The producer PID which is
printed to the console is part of the sensor-id for the measurement events,
which allows you to correlate.)

After giving up, the error handler logs an error and a list of discarded
messages. But this error handler also allows us to configure a
`ConsumerRecordRecoverer` which will be given the opportunity to recover
messages, one by one, after all retry attempts have been exhausted.

You can redo this experiment with the recovery option by running Spring boot app
with error handler `retry-with-backoff-recovery` instead of
`retry-with-backoff`. Are messages after the poision pill still lost ? See the
code for this in `RetryingErrorHandler`, which is a custom extension of the
Spring Kafka `DefaultErrorHandler`, but all it really does it set a recoverer
and back-off-spec.

If you can ensure your storage or processing is idempotent, you will be saving
yourself some trouble in these situations, so that multiple writes of the same
data is not a problem. The event store in the Kafka Sandbox Spring Boot app has
this property. In other words, strive for "at least once" message processing
semantics.


### Experiment: batch consumer error handling in Spring Kafka: recovery     <a name="spring-batch-error-5"/>

Notice in the code for our custom `RetryingErrorHandler` (enabled with
`retry-with-backoff-recovery` from previous experiment) that it has to take care
of storing valid events to the event store, which is also the main job of the
consumer code. So there is a duplication of efforts there. We can actually just
use the Spring Kafka `DefaultErrorHandler` with a recoverer, and make our batch
consumer throw `BatchListenerFailedException`, instead of direct causes. This
makes Spring aware of which record in a batch that failed, and so only the
problematic records need recovery handling.

Try this:

1. Go through steps 1-4 in the previous section, so that you end up with a
   poison pill null message in between other valid messages on the topic.
   
2. Start Spring Boot app with the recovering error handler and enable use of
   `BatchListenerFailedException` in consumer:

        ./boot-app --measurements.consumer.error-handler=recovering --measurements.consumer.useBatchListenerFailedException=true
        
Notice in the the logs as the consumer first reports receiving the batch of
messages. An exception is thrown because of the `null` message. The next time
the consumer is invoked, it receives fewer messages in the batch, because the
Spring error handler has automatically committed all messages up to, but not
including, the failing message. So those previous messages need not be attempted
again.

It then tries to run the rest of the batch up until retries are exhausted, then
it invokes the custom recovery handler. This recovery handler just logs that the
null message is discarded. After that, the last messages in the batch are handed
over to the consumer, which stores those successfully.

End result: all valid messages that could be stored, have been stored, and the
poison pill null message was simply skipped. Also, there is a performance
benefit when comparing to the `RetryingErrorHandler`, since the valid messages
are not written multiple times to the store during retries/recovery.


### What about transient failures when storing events ?          <a name="spring-batch-error-6"/>

If a valid message fails to be written into the event store, an `IOException` of
some kind is typically thrown. This may be just a temporary condition, so it
often makes sense to just retry until all messages in a batch are successfully
written. The recovering error handler deals with this situation in exactly that
way, by simply throwing an exception from the record recovery code.

For this to work, the consumer listener must wrap exceptions in the Spring
exception `BatchListenerFailedException` to communicate to the error handler
which record in the batch failed. The error handler will take care of the rest.

You can try this of course:

1. Ensure our test topic is empty with `./run deltopic measurements`.

2. Start Spring boot app with an event store that sometimes fail and using
   the recovering error handler:

        ./boot-app --measurements.consumer.error-handler=recovering --measurements.event-store.failure-rate=0.5 --measurements.consumer.useBatchListenerFailedException=true

3. Start a producer in another terminal:

        ./run producer

4. Let it run for a little while and watch Spring Boot app logs. You will see
   errors when event store fails.

5. Stop producer with `CTRL+C`, noting how many messages it sent to the Kafka
   topic (it is logged when it quits). Notice in Spring boot logs that it
   continues working on batches, which is also getting smaller each time, as
   more messages are *eventually written successfully* to the event store.

6. Navigate to http://localhost:8080/measurements.html and look at how many
   events have been successfully written to the event store. There should be
   *none missing*, even though the store failed half of the write attempts !
   
### Handling failure to deserialize messages in batch consumer       <a name="spring-batch-error-7"/>

Since the deserialization step happens before our consumer listener gets the
messages, it needs to be handled in a special way. Spring has designed the
`ErrorHandlingDeserializer` Kafka deserializer for this purpose. It catches
failed attempts at deserializing from a delegated deserializer class.

By default, our Spring Boot app is setup to handle failures with deserializing
values.

Test it out:

1. Start Spring boot app:

        ./boot-app
        
2. Then send badly formatted data to the 'measurements' topic:

        ./run string-producer '}badjson' measurements
        
Watch logs in Spring boot app. You'll see an error logged with information about
the record that failed. This is accomplished by using Spring-Kafka
`ErrorHandlingDeserializer` which delegates to the `JsonDeserializer`. Code for
setting this up is found in `MeasurementsConfig`. Code for extracting the error
in the batch listener can be found in `MeasurementsConsumer`.

Now try disabling handling of deserialization errors:

1. Start Spring boot app:

        ./boot-app --measurements.consumer.handle-deserialization-error=false
        
2. Send badly formatted data to the 'measurements' topic:

        ./run string-producer '}badjson' measurements

You will notice that the Spring boot app now behaves in an undesirable way, both
because it will never progress past the bad record (unless it is unassigned from
the topic-partition and another consumer in the same group picks up the bad
record), and because it does no backoff delaying with the default error handler,
so the listener container keeps failing over and over rapidly.

To improve things slightly, you can select an error handler that does
backoff-delaying, like `recovering`:

    ./boot-app --measurements.consumer.handle-deserialization-error=false --measurements.consumer.error-handler=recovering

It will not be able to progress beyond the error, but at least it does delay
between attempts to avoid flodding logs and using up a lot of resources.

The best practice is to actually handle deserialization errors, like
demonstrated earlier. Or you will have to write a custom error handler which can
deal with `SerializationException`, which the `DefaultErrorHandler` cannot
process.

### Stopping consumer on fatal errors            <a name="spring-batch-error-8"/>

Sometimes an error is definitively not recoverable and some form of manual
intervention or application restart is required. An example may be that the
consumer is not authorized to write data to the target data store due to invalid
credentials. In such cases you can look to `CommonContainerStoppingErrorHandler`
in Spring Kafka, which by default simply shuts down the consumer on any error
thrown from the batch processing in the listener. By extending this class, you
can test the type of error and delegate to the super class if the error is
deemed fatal.

To see how an *unmodified* `CommonContainerStoppingErrorHandler` works, you can
do the following:

1. Ensure a poison pill message is present on topic:

        ./run null-producer

2. Start a regular producer and let it run:

        ./run producer

2. Start Spring boot app with `stop-container` error handler:

        ./boot-app --measurements.consumer.error-handler=stop-container

Check logs. You will see that as soon as the posion pill message is encountered
the error handler kicks into action and stops the Spring message listener
container. Then all activity stops and the topic-partition is unassigned from
the consumer. As this app does not provide any way of re-starting the consumer,
the app itself must be restarted to try again.


### Further reading and experimentation

You can investigate and modify code in `MeasurementsConfig` and in package
`no.nav.kafka.sandbox.measurements.errorhandlers` to experiment further. Spring
has a large number of options and customizability with regard to error handling
in Kafka consumers (see `CommonErrorHandler` and all of its sub-types).

Also see https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html#error-handlers


## Tuning logging to get more details                    <a name="log-tuning"/>

If you would like to see the many technical details that the Kafka clients emit,
you can set the log level of the Apache Kafka clients in the file
`src/main/resources/simplelogger.properties`. It is by default `WARN`, but
`INFO` will output much more information. For the Spring Boot application,
logging setup is very standard and can be adjusted according to Spring
documentation.


## Unit/integration tests with `DockerComposeEnv`       <a name="integration-tests"/>

The class `DockerComposeEnv` can be used to manage a temporary docker-compose
environment for unit tests. It makes it simple to bring up/down
docker-compose-configurations between tests and handles port assignments and
other boring details automatically. It also ensures that the Docker resources
have unique names that should not conflict with other containers. It requires
the `docker compose` command to function, but has no other dependencies. A basic
compose configuration for Kafka services is stored in
`src/test/resources/DockerComposeKafkaEnv.yml`, which is used by
`KafkaSandboxTest`.

See example of usage in `KafkaSandboxTest`.

## Tips to clean up ephemeral Docker containers and networks

When developing unit tests, sometimes things go awry and the Docker containers
comprising the temporary Kafka environments are not cleaned up. Here are a few
tips to keep things tidy and free resources.

To stop and erase all `KafkaDockerComposeEnv`-created Docker containers and
networks, use the following commands:

    docker rm -fv $(docker ps -aq -f name=broker-test- -f name=zookeeper-test-)
    docker network rm $(docker network ls -f name=kafkadockercomposeenv -q)


## Using kafkacat to inspect Kafka topics                  <a name="kafkacat"/>

When working with Kafka, a very useful command line tool is
[kafkacat](https://github.com/edenhill/kafkacat). It is a light weight, but
powerful Kafka client that supports many options.

A typical installation on Debian-ish Linux can be accomplished with:

    sudo apt install kafkacat
    
Or Mac:

    brew install kafkacat

A tool which demonstrates use of kafkacat is included in the source code of this
repository. It can be found `clients-kafkacat/topic-tail` and is a "tail"-like
command to show metadata for the latest records on a Kafka topic. It requires
Python 3.5+ and kafkacat to run.

Try it out:

1. Ensure measurements producer is running: `./run producer`

2. Tail topic example:

        $ clients-kafkacat/topic-tail -f measurements
        [measurements-0] key sensor-9950, sz   127, ts 2020-10-06T22:50:53.755, offset 0
        [measurements-0] key sensor-9950, sz   127, ts 2020-10-06T22:50:55.633, offset 1
        [measurements-0] key sensor-9950, sz   127, ts 2020-10-06T22:50:57.608, offset 2
        

## Using official Kafka command line tools                <a name="kafka-cmds"/>

You can connect to the Docker container running the Kafka broker and get access
to some interesting command line tools:

    docker exec -it broker /bin/bash -i
    
Then type "kafka-" and press TAB a couple of times to find the commands
available.

## Ending words

Hopefully you will have learned a thing or two by following this guide. Any
feedback, suggestions, bug reports, etc. can be directed at the author: oyvind@stegard.net
