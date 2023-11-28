package no.nav.kafka.sandbox;

import no.nav.kafka.sandbox.DockerComposeEnv.DockerComposeCommand;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DockerComposeEnvTest {

    @Test
    void dockerComposeCommand() {
        assertEquals(List.of("docker-compose", "--no-ansi"),
                new DockerComposeCommand("docker-compose", new String[0], "1.28").executableAndDefaultArgs());
        assertEquals(List.of("docker-compose", "--no-ansi"),
                new DockerComposeCommand("docker-compose", new String[0], "1").executableAndDefaultArgs());

        assertEquals(List.of("docker-compose", "--ansi", "never"),
                new DockerComposeCommand("docker-compose", new String[0], "1.29").executableAndDefaultArgs());
        assertEquals(List.of("docker", "compose", "--ansi", "never"),
                new DockerComposeCommand("docker", new String[]{"compose"}, "2").executableAndDefaultArgs());
    }

}
