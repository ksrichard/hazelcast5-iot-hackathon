package com.hazelcast.hackathon;

public class StatResponse {

    public StatResponse(Double streamTempCur, Double streamTempAvg, Double streamTempMin,
        Double streamTempMax) {

        this.streamTempCur = streamTempCur;
        this.streamTempAvg = streamTempAvg;
        this.streamTempMin = streamTempMin;
        this.streamTempMax = streamTempMax;
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

}
