package no.nav.kafka.sandbox.data;

import java.util.List;


/**
 *
 * @param <T> some type of event
 */
public interface EventStore<T> {

    /**
     *
     * @param event an immutable event object
     */
    void storeEvent(T event);

    /**
     * @return all events from oldest to most recently added.
     */
    List<T> fetchEvents();

}
