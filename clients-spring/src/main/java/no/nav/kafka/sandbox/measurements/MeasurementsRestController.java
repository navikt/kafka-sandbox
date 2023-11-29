package no.nav.kafka.sandbox.measurements;

import no.nav.kafka.sandbox.data.EventStore;
import no.nav.kafka.sandbox.messages.Measurements;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.List;

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
    public List<Measurements.SensorEvent> getMeasurements(@RequestParam(value = "after", required = false, defaultValue = "1970-01-01T00:00Z")
                                                          @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime after) {

        return eventStore.fetchEvents().stream()
                .filter(e -> e.getTimestamp().isAfter(after))
                .toList();
    }

}
