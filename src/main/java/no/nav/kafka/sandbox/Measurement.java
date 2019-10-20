package no.nav.kafka.sandbox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class Measurement {

    private final String deviceId;
    private final String measureType;
    private final String unitType;
    private final LocalDateTime timestamp;
    private final Integer value;

    @JsonCreator
    public Measurement(@JsonProperty("deviceId") String deviceId,
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
        return "Measurement{" +
                "deviceId='" + deviceId + '\'' +
                ", measureType='" + measureType + '\'' +
                ", unitType='" + unitType + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }

}
