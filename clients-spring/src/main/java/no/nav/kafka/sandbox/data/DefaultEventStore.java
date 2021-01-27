package no.nav.kafka.sandbox.data;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

public class DefaultEventStore<T> implements EventStore<T> {

    private final Deque<T> events = new LinkedList<>();
    private final int maxSize;
    private final boolean failOnMaxSize;

    public DefaultEventStore(int maxSize, boolean failOnMaxSize) {
        this.maxSize = maxSize;
        this.failOnMaxSize = failOnMaxSize;
    }

    @Override
    public synchronized boolean storeEvent(T event) {
        if (events.contains(event)) {
            return false;
        }
        while (events.size() >= maxSize) {
            if (failOnMaxSize) {
                throw new IllegalStateException("Store has reached max capacity of " + events.size() + " events");
            }
            events.removeFirst();
        }
        events.add(event);
        return true;
    }

    @Override
    public synchronized boolean removeIf(Predicate<T> p) {
        return events.removeIf(p);
    }

    @Override
    public synchronized List<T> fetchEvents() {
        return new ArrayList<>(events);
    }
}
