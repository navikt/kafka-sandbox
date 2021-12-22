package no.nav.kafka.sandbox.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiRenderer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Measurements {

    public static class SensorEvent {

        private final String deviceId;
        private final String measureType;
        private final String unitType;
        private final LocalDateTime timestamp;
        private final Integer value;

        @JsonCreator
        public SensorEvent(@JsonProperty("deviceId") String deviceId,
                           @JsonProperty("measureType") String measureType,
                           @JsonProperty("unitType") String unitType,
                           @JsonProperty("timestamp") LocalDateTime timestamp,
                           @JsonProperty("value") Integer value) {
            this.deviceId = Objects.requireNonNull(deviceId);
            this.measureType = Objects.requireNonNull(measureType);
            this.unitType = Objects.requireNonNull(unitType);
            this.timestamp = Objects.requireNonNull(timestamp);
            this.value = Objects.requireNonNull(value);
        }

        public String getDeviceId() {
            return deviceId;
        }

        public String getMeasureType() {
            return measureType;
        }

        public String getUnitType() {
            return unitType;
        }

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        public Integer getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "SensorEvent{" +
                    "deviceId='" + deviceId + '\'' +
                    ", measureType='" + measureType + '\'' +
                    ", unitType='" + unitType + '\'' +
                    ", timestamp=" + timestamp +
                    ", value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SensorEvent that = (SensorEvent) o;
            return Objects.equals(deviceId, that.deviceId)
                    && Objects.equals(measureType, that.measureType)
                    && Objects.equals(unitType, that.unitType)
                    && Objects.equals(timestamp, that.timestamp)
                    && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deviceId, measureType, unitType, timestamp, value);
        }
    }

    /**
     * @param maxNumberOfElements max number of elements to supply
     * @return a sensor event supplier providing up to n values.
     * @throws NoSuchElementException when no more events can be supplied.
     */
    public static Supplier<SensorEvent> eventSupplier(final int maxNumberOfElements) {
        final AtomicInteger counter = new AtomicInteger();
        return () -> {
            if (counter.incrementAndGet() > maxNumberOfElements) {
                throw new NoSuchElementException("No more data can be supplied");
            }
            return generateEvent();
        };
    }

    /**
     * @return a sensor event supplier providing infinite number of values with a delay.
     */
    public static Supplier<SensorEvent> delayedInfiniteEventSupplier() {
        return () -> {
            try {
                Thread.sleep((long) (Math.random() * 1000) + 1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return generateEvent();
        };
    }

    /**
     * @return a single random sensor event
     */
    public static SensorEvent generateEvent() {
        final int temp = (int)(Math.random()*20 + 19);
        return new SensorEvent("sensor-" + ProcessHandle.current().pid(),
                "temperature", "celcius", LocalDateTime.now(), temp);
    }

    public static void sensorEventToConsole(SensorEvent m) {
        final String ansiOutput = AnsiRenderer.render(String.format(
                "@|cyan Device|@: @|magenta,bold %s|@, value: @|blue,bold %d\u00B0|@ %s, timestamp: @|green %s|@",
                m.getDeviceId(), m.getValue(), m.getUnitType(), m.getTimestamp()));

        AnsiConsole.out().println(ansiOutput);
    }

}
