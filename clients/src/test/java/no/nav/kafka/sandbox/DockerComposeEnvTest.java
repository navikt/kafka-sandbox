package no.nav.kafka.sandbox;

import no.nav.kafka.sandbox.DockerComposeEnv.DockerComposeExecutable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DockerComposeEnvTest {

    @Test
    public void testDockerComposeExecutable() {
        assertEquals(List.of("docker-compose", "--no-ansi"),
                new DockerComposeExecutable("docker-compose", "1.28").executableAndDefaultArgs());
        assertEquals(List.of("docker-compose", "--no-ansi"),
                new DockerComposeExecutable("docker-compose", "1").executableAndDefaultArgs());

        assertEquals(List.of("docker-compose", "--ansi", "never"),
                new DockerComposeExecutable("docker-compose", "1.29").executableAndDefaultArgs());
        assertEquals(List.of("docker-compose", "--ansi", "never"),
                new DockerComposeExecutable("docker-compose", "2").executableAndDefaultArgs());
    }

}
