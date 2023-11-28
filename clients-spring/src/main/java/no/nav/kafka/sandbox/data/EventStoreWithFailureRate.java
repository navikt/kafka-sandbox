package no.nav.kafka.sandbox.data;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p></p>Simulate event store that sometimes fails to store values, depending on configured failure rate.
 *
 * <p>Can be used to test error handling in Spring Kafka.</p>
 *
 * @param <T> store element type
 */
public class EventStoreWithFailureRate<T> extends DefaultEventStore<T> {

    private final FailureRateStrategy<T> failureRate;
    private final Function<T, Throwable> exceptionSupplier;

    /**
     *
     * @param maxSize maximum size of event store, oldest value is discarded when number of elements goes beyond limit.
     * @param failureRate a pluggable strategy implementing how often to fail store calls
     * @param exceptionSupplier code to create a synthentic exception to throw when failure should occur. This function
     *                         will be provided with the value to store as argument and should produce a {@code Throwable} of some type.
     */
    public EventStoreWithFailureRate(int maxSize, boolean failOnMaxSize, FailureRateStrategy<T> failureRate, Function<T, Throwable> exceptionSupplier) {
        super(maxSize, failOnMaxSize);
        this.failureRate = failureRate;
        this.exceptionSupplier = Objects.requireNonNull(exceptionSupplier, "exceptionSupplier cannot be null");
    }

    @Override
    public synchronized boolean storeEvent(T event) {
        if (failureRate.failureFor(event)) {
            sneakyThrow(exceptionSupplier.apply(event));
        }

        return super.storeEvent(event);
    }

    // https://stackoverflow.com/questions/14038649/java-sneakythrow-of-exceptions-type-erasure
    private static <E extends Throwable> RuntimeException sneakyThrow(Throwable e) throws E {
        throw (E)e;
    }

    @FunctionalInterface
    public interface FailureRateStrategy<T> {

        boolean failureFor(T event);

    }

    public static class FixedFailSuccessCountPattern<T> implements FailureRateStrategy<T> {

        private final int failCount;
        private final int totalCount;
        private final AtomicInteger counter = new AtomicInteger(0);

        private FixedFailSuccessCountPattern(int failCount, int totalCount) {
            if (failCount > totalCount) {
                throw new IllegalArgumentException("fail count cannot be bigger than total count");
            }
            this.failCount = failCount;
            this.totalCount = totalCount;
        }

        @Override
        public boolean failureFor(T t) {
            final int value = counter.getAndIncrement() % totalCount;
            return value < failCount;
        }

        public static <T> FixedFailSuccessCountPattern fromSpec(String spec) {
            Matcher matcher = Pattern.compile("([0-9]+)\\s*/\\s*([0-9]+)").matcher(spec);
            if (!matcher.find()) {
                throw new IllegalArgumentException("Failure rate expression should be on the form 'x/y', "
                        + "where x is number of times to fail and y is total number of times for a cycle.");
            }
            int failCount = Math.max(0 ,Integer.parseInt(matcher.group(1)));
            int totalCount = Math.max(0, Integer.parseInt(matcher.group(2)));

            return new FixedFailSuccessCountPattern<T>(failCount, totalCount);
        }

    }

    public static class AverageRatioRandom<T> implements FailureRateStrategy<T> {

        private final float failureRate;
        private final Random random = new Random();

        public AverageRatioRandom(float failureRate) {
            if (failureRate < 0 || failureRate > 1.0) {
                throw new IllegalArgumentException("failure rate must be a decimal number between 0.0 and 1.0");
            }
            this.failureRate = failureRate;
        }

        @Override
        public boolean failureFor(T event) {
            return random.nextFloat() < failureRate;
        }
    }

}
