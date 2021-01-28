package no.nav.kafka.sandbox.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.Objects;

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

    // Produces one synthentic temp event every 1-2 seconds
    public static SensorEvent acquireTemperatureSensorMeasurement() {
        // Throttling and some variance in timing
        try {
            Thread.sleep((long) (Math.random() * 1000) + 1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        final int temp = (int)(Math.random()*20 + 19);
        return new SensorEvent("sensor-" + ProcessHandle.current().pid(),
                "temperature", "celcius", LocalDateTime.now(), temp);
    }

    public static void sensorEventToConsole(SensorEvent m) {
        System.out.println(String.format("Device: %s, value: %d\u00B0 %s, timestamp: %s",
                m.getDeviceId(), m.getValue(), m.getUnitType(), m.getTimestamp()));
    }

}
