package no.nav.kafka.sandbox.data;

import java.util.List;
import java.util.function.Predicate;


/**
 *
 * @param <T> some type of event, preferably an immutable type.
 */
public interface EventStore<T> {

    /**
     *
     * @param event an immutable event object
     * @return {@code true} if event was stored, {@code false} if it already existed
     */
    boolean storeEvent(T event);

    /**
     * @return all events from oldest to most recently added.
     */
    List<T> fetchEvents();

    /**
     * Removes all stored events where predicate is {@code true}.
     * @return {@code true} if at least one element was removed
     */
    boolean removeIf(Predicate<T> p);

}
