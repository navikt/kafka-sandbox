package no.nav.kafka.sandbox.data;

import java.util.Objects;
import java.util.Random;
import java.util.function.Function;

/**
 * <p></p>Simulate event store that sometimes fails to store values, depending on configured failure rate.
 *
 * <p>Can be used to test error handling in Spring Kafka.</p>
 *
 * @param <T> store element type
 */
public class EventStoreWithFailureRate<T> extends DefaultEventStore<T> {

    private final float failureRate;
    private final Random randomizer = new Random();
    private final Function<T, Throwable> exceptionSupplier;

    /**
     *
     * @param maxSize maximum size of event store, oldest value is discarded when number of elements goes beyond limit.
     * @param failureRate a number between 0.0 and 1.0 indicating ratio of calls to {@link #storeEvent(Object)} that should fail
     * @param exceptionSupplier code to create a synthentic exception to throw when failure should occur. This function
     *                         will be provided with the value to store as argument and should produce a {@code Throwable} of some type.
     */
    public EventStoreWithFailureRate(int maxSize, boolean failOnMaxSize, float failureRate, Function<T, Throwable> exceptionSupplier) {
        super(maxSize, failOnMaxSize);
        this.failureRate = failureRate;
        this.exceptionSupplier = Objects.requireNonNull(exceptionSupplier, "exceptionSupplier cannot be null");
    }

    @Override
    public synchronized boolean storeEvent(T event) {
        if (randomizer.nextFloat() < failureRate) {
            throwSoftenedException(exceptionSupplier.apply(event));
        }
        return super.storeEvent(event);
    }

    private static RuntimeException throwSoftenedException(Throwable e) {
        return softenHelper(e);
    }

    private static <E extends Throwable> E softenHelper(Throwable e) throws E {
        throw (E)e;
    }

}
