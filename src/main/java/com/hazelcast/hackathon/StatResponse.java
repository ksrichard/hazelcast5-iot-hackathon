package com.hazelcast.hackathon;

public class StatResponse {

    public StatResponse(Double streamTempCur, Double streamTempAvg, Double streamTempMin,
        Double streamTempMax) {

        this.streamTempCur = decreasePrecision(streamTempCur);
        this.streamTempAvg = decreasePrecision(streamTempAvg);
        this.streamTempMin = decreasePrecision(streamTempMin);
        this.streamTempMax = decreasePrecision(streamTempMax);
    }

    private Double streamTempCur;

    private Double streamTempAvg;

    private Double streamTempMin;

    private Double streamTempMax;

    public Double getStreamTempCur() {
        return streamTempCur;
    }

    public Double getStreamTempAvg() {
        return streamTempAvg;
    }

    public Double getStreamTempMin() {
        return streamTempMin;
    }

    public Double getStreamTempMax() {
        return streamTempMax;
    }

    private Double decreasePrecision(Double value) {
        return Math.floor(value * 100) / 100;
    }

}
