package no.nav.kafka.sandbox.measurements;

import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.Measurements;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class MeasurementsRestController {

    private final EventStore<Measurements.SensorEvent> eventStore;

    public MeasurementsRestController(EventStore<Measurements.SensorEvent> sensorEventStore) {
        this.eventStore = sensorEventStore;
    }

    /**
     * @param after only include messages with a timestamp later than this value
     * @return messages from most oldest to most recent, optionally filtering by timestamp.
     */
    @GetMapping(path = "/measurements/api", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<Measurements.SensorEvent> getMeasurements(@RequestParam(value = "after", required = false, defaultValue = "1970-01-01T00:00")
                                                          @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime after) {
        return eventStore.fetchEvents().stream()
                .filter(e -> e.getTimestamp().isAfter(after))
                .collect(Collectors.toList());
    }

}
