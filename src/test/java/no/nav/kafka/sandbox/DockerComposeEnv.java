package no.nav.kafka.sandbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ServerSocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.ProcessBuilder.Redirect.appendTo;

/**
 * Support class for invoking docker-compose and waiting for services, typically to be used in tests.
 */
public final class DockerComposeEnv implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DockerComposeEnv.class);

    /**
     * Builder which is used to setup the docker-compose invocation.
     *
     * <p>You can use multiple ready-tests per environment. The {@link Builder#up() up method} will wait until
     * all ready-tests complete before returning the instance of {@code DockerComposeEnv}. The ready-tests
     * are executed in parallel.</p>
     */
    public static class Builder {
        private final String config;
        private String logdir = null;
        private final Map<String,String> env = new HashMap<>();
        private final List<Supplier<Boolean>> readyTests = new ArrayList<>();
        private int lastAutoPort = 39999;
        private int dockerComposeTimeoutSeconds = 1800;

        private Builder(String config) {
            this.config = Objects.requireNonNull(config);
        }

        /**
         * Add low level ready-test that checks if a single TCP port can be connected to.
         * @param host
         * @param port
         * @return this builder
         */
        public Builder readyWhenPortIsOpen(String host, int port) {
            this.readyTests.add(() -> {
                try (Socket s = new Socket(InetAddress.getByName(host), port)) {
                    log.info("Port is ready: {}:{}", host, port);
                    return true;
                } catch (IOException e) {
                    return false;
                }
            });
            return this;
        }

        /**
         * Same as {@link #readyWhenPortIsOpen(String, int)}, but uses a previously configured env variable
         * as the port, which can be a variable set with  {@link #addAutoPortVariable(String)}.
         * @param host typically "localhost"
         * @param portVariable name of env variable
         * @throws NullPointerException if env variable is not previously configured
         * @throws NumberFormatException if env variable is not parseable as an integer
         * @return this builder
         */
        public Builder readyWhenPortIsOpen(String host, String portVariable) {
            final int port = Integer.parseInt(env.get(portVariable));
            return readyWhenPortIsOpen(host, port);
        }

        /**
         * Add ready when an http-GET to a URL yields status code 2xx.
         * @param url
         * @return this builder
         */
        public Builder readyOnHttpGet2xx(String url) {
            if (!Objects.requireNonNull(url, "URL cannot be null").startsWith("http")) {
                throw new IllegalArgumentException("URL must start with 'http'");
            }
            this.readyTests.add(() -> {
                HttpURLConnection httpConnection = null;
                try {
                    httpConnection = (HttpURLConnection)new URL(url).openConnection();
                    httpConnection.setConnectTimeout(1000);
                    httpConnection.setReadTimeout(2000);
                    final int responseCode = httpConnection.getResponseCode();
                    if (responseCode >= 200 && responseCode < 300) {
                        log.info("Http-service is ready: {}", url);
                        return true;
                    } else {
                        return false;
                    }
                } catch (IOException e) {
                    return false;
                } finally {
                    try {
                        if (httpConnection != null) {
                            httpConnection.getInputStream().close();
                        }
                    } catch (IOException io) {}
                }
            });
            return this;
        }

        /**
         * Add test which is ready when an http-GET to a URL yields status code 2xx.
         * @param urlTemplate the URL, where "{VALUE}" is replaced by the value of a an env-variable, which
         *                    must be previously set and can be an auto-port-variable.
         * @param envVariable variable to use
         * @return this builder
         */
        public Builder readyOnHttpGet2xx(String urlTemplate, String envVariable) {
            return readyOnHttpGet2xx(urlTemplate.replaceAll(Pattern.quote("{VALUE}"), env.get(envVariable)));
        }

        /**
         * Add simple ready-test which simply returns {@code false} until the desired amount of time has elapsed since
         * the call to this method method.
         */
        public Builder readyAfter(long duration, TimeUnit timeUnit) {
            final AtomicLong start = new AtomicLong(-1);
            final long millisToWait = timeUnit.toMillis(duration);
            this.readyTests.add(() -> {
                if (start.compareAndSet(-1, System.currentTimeMillis())) {
                    return false;
                }
                if (System.currentTimeMillis() - start.get() > millisToWait) {
                    log.info("Waited for at least {} milliseconds, signalling ready", millisToWait);
                    return true;
                } else {
                    return false;
                }
            });
            return this;
        }

        /**
         * Add environment variable to export to docker-compose process.
         * <p>The value of this variable can later be retrieved with {@link #getEnvVariable(String)}</p>
         * @param variable name, not null
         * @param value value
         * @return the builder
         */
        public Builder addEnvVariable(String variable, Object value) {
            env.put(Objects.requireNonNull(variable), value != null ? value.toString() : "null");
            return this;
        }

        /**
         * Get value of a previously set variable, which can be an auto-port-variable.
         * @param variable
         * @return
         */
        public String getEnvVariable(String variable) {
            return env.get(variable);
        }

        /**
         * Expose an environment variable having a random (currently) free port number as value.
         * <p>The value of this variable can later be retrieved with {@link #getEnvVariable(String)}</p>
         * @param portVariable an environment variable name
         * @return this builder
         */
        public Builder addAutoPortVariable(String portVariable) {
            final int freePort = chooseFreePort(lastAutoPort);
            this.env.put(Objects.requireNonNull(portVariable), String.valueOf(freePort));
            lastAutoPort = freePort;
            return this;
        }

        /**
         * Add a custom ready test as a supplier which can test if any external docker services are ready.
         * <p>The builder instance itself is provided as the function argument to be able to extract
         * values from auto-port variables, see {@link #getEnvVariable(String)}.</p>
         * <p>The ready-test-supplier be called potentially multiple times at some interval and should
         * supply {@code true} when a service is ready.</p>
         * <p>By default, a ready-test which simply waits 5 seconds is used.</p>
         * @param readyTestSupplier a function which accepts this build as argument and returns a new ready-test supplier
         * @return this builder
         */
        public Builder withCustomReadyTest(Function<Builder,Supplier<Boolean>> readyTestSupplier) {
            Supplier<Boolean> readyTest = readyTestSupplier.apply(this);
            if (readyTest != null) {
                this.readyTests.add(readyTest);
            }
            return this;
        }

        /**
         * Set a directory where docker-compose stdout/stderr logs will be stored.
         * <p>Default is nothing, and no logs are kept.</p>
         * @param directoryPath
         * @return this builder
         */
        public Builder dockerComposeLogDir(String directoryPath) {
            this.logdir = directoryPath;
            return this;
        }

        /**
         * Set wait limit in seconds for docker-compose to finish bringing up containers. This time may involve
         * Docker downloading images from the internet, take that into account.
         * <p>Default value: {@code 1800} seconds</p>
         * @return this builder
         */
        public Builder dockerComposeTimeout(int timeoutSeconds) {
            this.dockerComposeTimeoutSeconds = timeoutSeconds;
            return this;
        }

        /**
         * Bring up configured env. This call will block for some amount of time to invoke docker-compose, and then
         * to wait using any set ready-test.
         * @return a hopefully ready-to-use docker-compose environment
         */
        public DockerComposeEnv up() throws Exception {
            if (readyTests.isEmpty()) {
                readyAfter(5, TimeUnit.SECONDS);
            }
            return new DockerComposeEnv(this.config, this.dockerComposeTimeoutSeconds, this.logdir, this.env, this.readyTests).up();
        }
    }

    /**
     * Get a builder for a docker-compose test environment.
     * @param dockerComposeConfigFile
     * @return
     */
    static DockerComposeEnv.Builder builder(String dockerComposeConfigFile) {
        return new Builder(dockerComposeConfigFile);
    }

    private final Path dockerComposeLogDir;
    private final String configFile;
    private final long timeoutSeconds;
    private final Map<String,String> env;
    private final List<Supplier<Boolean>> readyTests;

    private DockerComposeEnv(String configFile, long timeoutSeconds, String logdir, Map<String,String> env, List<Supplier<Boolean>> readyTests) {
        if (logdir != null) {
            File dir = new File(logdir);
            if (!dir.isDirectory() || !dir.canWrite()) {
                throw new IllegalArgumentException("Invalid directory for logs (does not exist or is not writable): " + logdir);
            }
            this.dockerComposeLogDir = dir.toPath();
        } else {
            this.dockerComposeLogDir = null;
        }
        this.configFile = configFile;
        this.timeoutSeconds = timeoutSeconds;
        this.env = env;
        this.readyTests = readyTests;
    }

    /**
     * Factory for constructing a new docker-compose test environment.
     * <p>Waits for all configured ready-tests to complete before returning.</p>
     *
     * @return the new instance
     * @throws Exception in case something goes awry during setup.
     */
    private DockerComposeEnv up() throws Exception {
        log.info("Bringing up local docker-compose environment, max wait={} seconds, env={}", this.timeoutSeconds, this.env);
        dockerCompose("up", "-d").start().onExit().thenAccept(process -> {
                    if (process.exitValue() != 0) {
                        throw new RuntimeException("docker-compose failed with status " + process.exitValue());
                    }
                }).get(this.timeoutSeconds, TimeUnit.SECONDS);

        log.info("Waiting for {} ready-test(s) to complete ..", readyTests.size());
        List<CompletableFuture<Void>> allReadyTestsComplete = readyTests.stream().map(
                test -> CompletableFuture.runAsync(()-> {
                            int count = 0;
                            while (!test.get() && count++ < 100) {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException ie) {
                                    throw new RuntimeException("Interrupted while waiting for ready-test");
                                }
                            }
                            if (count > 100) {
                                throw new RuntimeException("Ready-test failed");
                            }
                        }
                )).collect(Collectors.toList());

        try {
            CompletableFuture.allOf(allReadyTestsComplete.toArray(new CompletableFuture[]{}))
                    .thenRun(() -> log.info("All ready-tests completed."))
                    .get(240, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("At least one ready-test failed, or we timed out while waiting", e);
            down();
            throw e;
        }

        return this;
    }

    /**
     * Return value of a registered environment variable.
     * @param variable the variable
     * @return the value, or {@code null} if unknown
     */
    public String getEnvVariable(String variable) {
        return env.get(variable);
    }

    /**
     * Take down local docker environment using docker-compose.
     * @throws Exception when something bad happens, in which case you will have to clean up manually.
     */
    public void down() throws Exception {
        log.info("Taking down local docker-compose environment ..");
        dockerCompose("down", "--volumes").start().onExit().get(120, TimeUnit.SECONDS);
    }

    /**
     * Test if docker-compose can be executed
     * @return {@code true} on success
     */
    public static boolean dockerComposeAvailable() {
        try {
            findDockerComposeExecutableName();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private ProcessBuilder dockerCompose(String...composeCommandAndArgs) throws Exception {
        List<String> commandArgs = new ArrayList<>();
        commandArgs.addAll(List.of(findDockerComposeExecutableName(), "--no-ansi", "--log-level", "ERROR"));
        commandArgs.addAll(List.of("-f", composeFileAsPath(this.configFile).toString(), "-p", dockerComposeProjectName()));
        commandArgs.addAll(Arrays.asList(composeCommandAndArgs));
        ProcessBuilder pb = new ProcessBuilder(commandArgs);
        this.env.forEach((k,v) -> {
            pb.environment().put(k, v);
        });
        if (this.dockerComposeLogDir != null){
            pb.redirectOutput(appendTo(this.dockerComposeLogDir.resolve("DockerComposeEnv-" + obtainPid() + "-stdout.log").toFile()));
            pb.redirectError(appendTo(this.dockerComposeLogDir.resolve("DockerComposeEnv-" + obtainPid() + "-stderr.log").toFile()));
        } else {
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        }

        return pb;
    }

    private String dockerComposeProjectName() {
        return "DockerComposeEnv-test-" + obtainPid();
    }

    private static Path composeFileAsPath(String filePath) {
        return Path.of(filePath);
    }

    private static String findDockerComposeExecutableName() {
        for (String executable : List.of("/usr/bin/docker-compose", "/usr/local/bin/docker-compose", "/usr/local/sbin/docker-compose",
                "docker-compose", "docker-compose.exe")) {
            try {
                if (new ProcessBuilder(executable, "--version").start().onExit().get().exitValue() == 0) {
                    return executable;
                }
            } catch (Exception io) {
            }
        }
        throw new IllegalStateException("Could not find or execute docker-compose command on operating system");
    }

    private static long obtainPid() {
        return ProcessHandle.current().pid();
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
