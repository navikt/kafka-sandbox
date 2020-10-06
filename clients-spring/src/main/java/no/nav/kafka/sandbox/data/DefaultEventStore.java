package no.nav.kafka.sandbox.data;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class DefaultEventStore<T> implements EventStore<T> {

    private final Deque<T> events = new LinkedList<>();
    private final int maxSize;

    public DefaultEventStore(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public synchronized void storeEvent(T event) {
        while (events.size() >= maxSize) {
            events.removeFirst();
        }
        events.add(event);
    }

    @Override
    public synchronized List<T> fetchEvents() {
        return new ArrayList<>(events);
    }
}
