package no.nav.kafka.sandbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ServerSocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.ProcessBuilder.Redirect.appendTo;

/**
 * Simplest way to get Kafka running for tests.
 *
 * <p>Requires a working Docker installation on localhost and the 'docker-compose' command available in operating system PATH.</p>
 */
class KafkaDockerComposeEnv implements AutoCloseable {

    final int kafkaPort;

    private static final Logger log = LoggerFactory.getLogger(KafkaDockerComposeEnv.class);

    private KafkaDockerComposeEnv() {
        this.kafkaPort = chooseFreePort(39999);
    }

    /**
     * Factory for constructing a new instance and bring up local Kafka broker using docker-compose.
     * <p>Kafka port can be access by looking at {@link KafkaDockerComposeEnv#kafkaPort}</p>
     * @return the new instance
     * @throws Exception in case something goes awry during setup.
     */
    static KafkaDockerComposeEnv up() throws Exception {
        KafkaDockerComposeEnv kafkaEnv = new KafkaDockerComposeEnv();
        log.info("Bringing up local Kafka environment, broker port is " + kafkaEnv.kafkaPort + " ..");
        kafkaEnv.dockerCompose("up", "-d").start().toHandle().onExit()
                .thenRun(kafkaEnv::waitForKafkaBroker)
                .get(120, TimeUnit.SECONDS);
        return kafkaEnv;
    }

    /**
     * Take down local Kafka environment using docker-compose.
     * @throws Exception when something bad happens, in which case you will have to clean up manually.
     */
    void down() throws Exception {
        log.info("Taking down local docker-compose Kafka environment ..");
        dockerCompose("down", "--volumes").start().toHandle().onExit().get(120, TimeUnit.SECONDS);
    }

    /**
     * Test if docker-compose can be executed
     * @return {@code true} on success
     */
    static boolean dockerComposeAvailable() {
        try {
            findDockerComposeExecutableName();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void waitForKafkaBroker() {
        int attempt = 1;
        while (attempt++ <= 6 && !canConnectToKafkaBrokerPort()) {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException ie) {
                break;
            }
        }
    }

    // Only shallow test which waits for TCP-connectability. (Could be expanded to test using Kafka AdminClient.)
    private boolean canConnectToKafkaBrokerPort() {
        try (Socket s = new Socket(InetAddress.getLocalHost(), kafkaPort)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private ProcessBuilder dockerCompose(String...composeCommandAndArgs) throws Exception {
        List<String> commandArgs = new ArrayList<>();
        commandArgs.addAll(List.of(findDockerComposeExecutableName(), "--no-ansi", "--log-level", "ERROR"));
        commandArgs.addAll(List.of("-f", resolveDockerComposeFile().toString(), "-p", dockerComposeProjectName()));
        commandArgs.addAll(Arrays.asList(composeCommandAndArgs));
        ProcessBuilder pb = new ProcessBuilder(commandArgs);

        pb.environment().put("KAFKA_PORT", String.valueOf(kafkaPort));
        pb.redirectOutput(appendTo(new File("target/KafkaDockerComposeEnv-" + kafkaPort + "-stdout.log")));
        pb.redirectError(appendTo(new File("target/KafkaDockerComposeEnv-" + kafkaPort + "-stderr.log")));

        return pb;
    }

    private String dockerComposeProjectName() {
        return "KafkaDockerComposeEnv-test-" + kafkaPort;
    }

    private static Path resolveDockerComposeFile() throws Exception {
        return Paths.get(KafkaDockerComposeEnv.class.getResource("/KafkaDockerComposeEnv.yml").toURI());
    }

    private static String findDockerComposeExecutableName() {
        for (String executable : List.of("docker-compose.exe", "docker-compose")) {
            try {
                if (new ProcessBuilder(executable, "--version").start().onExit().get().exitValue() == 0) {
                    return executable;
                }
            } catch (Exception io) {
            }
        }
        throw new IllegalStateException("Could not find or execute docker-compose command on operating system");
    }

    private static int chooseFreePort(int higherThan) {
        int next = Math.min(65535, higherThan + 1 + (int)(Math.random()*100));
        do {
            try (ServerSocket s = ServerSocketFactory.getDefault().createServerSocket(
                    next, 1, InetAddress.getLocalHost())) {
                return s.getLocalPort();
            } catch (IOException e) {
            }
        } while ((next += (int)(Math.random()*100)) <= 65535);
        throw new IllegalStateException("Unable to find free network port on localhost");
    }

    @Override
    public void close() throws Exception {
        down();
    }
}
