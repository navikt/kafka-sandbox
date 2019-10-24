package no.nav.kafka.sandbox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

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
            this.deviceId = deviceId;
            this.measureType = measureType;
            this.unitType = unitType;
            this.timestamp = timestamp;
            this.value = value;
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
    }

    // Produces one synthentic temp event every 1-2 seconds
    static SensorEvent acquireTemperatureSensorMeasurement() {
        // Throttling and some variance in timing
        try {
            Thread.sleep((long) (Math.random() * 1000) + 1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        final int temp = (int)(Math.random()*20 + 19);
        return new SensorEvent("sensor-" + Bootstrap.obtainPid(),
                "temperature", "celcius", LocalDateTime.now(), temp);
    }

    static void sensorEventToConsole(SensorEvent m) {
        System.out.println(String.format("Device: %s, value: %d\u00B0 %s, timestamp: %s",
                m.getDeviceId(), m.getValue(), m.getUnitType(), m.getTimestamp()));
    }

}
