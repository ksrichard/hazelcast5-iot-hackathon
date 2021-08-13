package com.hazelcast.hackathon;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.contrib.mqtt.MqttSources;
import com.hazelcast.jet.contrib.mqtt.Subscription;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hivemq.embedded.EmbeddedHiveMQ;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.net.MalformedURLException;
import java.util.AbstractMap;

@QuarkusMain
public class Application {

    public static final String TEMPERATURE_MQTT_TOPIC_NAME = "temp";

    public static final String TEMPERATURE_MAP_NAME = "temp";

    public static final String TEMPERATURE_STATS_MAP_NAME = "temp_stats";

    public static final String TEMP_STREAM_STATS_AVERAGE_MAP_KEY = "temp_stream_avg";

    public static final String TEMP_STREAM_STATS_MIN_MAP_KEY = "temp_stream_min";

    public static final String TEMP_STREAM_STATS_MAX_MAP_KEY = "temp_stream_max";

    public static final String MQTT_INGESTION_JOB_NAME = "mqtt-ingestion";

    public static void main(String[] args) throws MalformedURLException {
        // start Hz client
        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient();

        IMap<Long, Double> temperaturesMap = hzClient.getMap(TEMPERATURE_MAP_NAME);
        IMap<String, Double> tempStatsMap = hzClient.getMap(TEMPERATURE_STATS_MAP_NAME);

        tempStatsMap.addEntryListener((EntryUpdatedListener<String, Double>) event -> {
            if (event.getKey().equalsIgnoreCase(TEMP_STREAM_STATS_AVERAGE_MAP_KEY)) {
                System.out.println("AVG stream temperature: " + event.getValue());
            }
            if (event.getKey().equalsIgnoreCase(TEMP_STREAM_STATS_MIN_MAP_KEY)) {
                System.out.println("MIN stream temperature: " + event.getValue());
            }
            if (event.getKey().equalsIgnoreCase(TEMP_STREAM_STATS_MAX_MAP_KEY)) {
                System.out.println("MAX stream temperature: " + event.getValue());
            }
        }, true);

        // start MQTT
        EmbeddedHiveMQ hiveMQ = EmbeddedHiveMQ.builder().build();
        hiveMQ.start().join();

        // MQTT ingestion pipeline
        startMqttIngestionPipeline(hzClient, temperaturesMap, tempStatsMap);

        Quarkus.run(args);
    }

    private static void startMqttIngestionPipeline(HazelcastInstance hzClient, IMap<Long, Double> temperatureMap,
        IMap<String, Double> temperatureStatsMap) {
        StreamSource<String> tempSource = MqttSources.builder()
            .clientId("jet-consumer")
            .broker("tcp://127.0.0.1:1883")
            .topic(TEMPERATURE_MQTT_TOPIC_NAME)
            .qualityOfService(Subscription.QualityOfService.AT_LEAST_ONCE)
            .autoReconnect()
            .keepSession()
            .mapToItemFn((topic, message) -> new String(message.getPayload()))
            .build();

        // MQTT processing pipeline
        Pipeline mqttIngestionPipeline = Pipeline.create();
        StreamStage<Double> streamStage = mqttIngestionPipeline.readFrom(tempSource)
            .withoutTimestamps()
            .filter(NumberUtils::isCreatable)
            .map(Double::parseDouble)
            .peek()
            .setLocalParallelism(4);

        streamStage.writeTo(Sinks.mapWithUpdating(
                temperatureMap,
                aDouble -> System.currentTimeMillis(),
                (oldValue, newValue) -> newValue));

        streamStage.rollingAggregate(AggregateOperations.averagingDouble(value -> value))
            .map(value -> new AbstractMap.SimpleEntry<>(TEMP_STREAM_STATS_AVERAGE_MAP_KEY, value))
            .writeTo(Sinks.map(temperatureStatsMap));

        streamStage.rollingAggregate(AggregateOperations.minBy(Double::compareTo))
            .map(value -> new AbstractMap.SimpleEntry<>(TEMP_STREAM_STATS_MIN_MAP_KEY, value))
            .writeTo(Sinks.map(temperatureStatsMap));

        streamStage.rollingAggregate(AggregateOperations.maxBy(Double::compareTo))
            .map(value -> new AbstractMap.SimpleEntry<>(TEMP_STREAM_STATS_MAX_MAP_KEY, value))
            .writeTo(Sinks.map(temperatureStatsMap));

        JobConfig jobConfig = getJobConfig(MQTT_INGESTION_JOB_NAME);
        Job mqttIngestionJob = hzClient.getJet().getJob(MQTT_INGESTION_JOB_NAME);
        if (mqttIngestionJob != null) {
            mqttIngestionJob.cancel();
        }
        hzClient.getJet().newJob(mqttIngestionPipeline, jobConfig);
    }

    private static JobConfig getJobConfig(String jobName) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        jobConfig.addClass(Application.class);
        jobConfig.addJar(new File("../src/main/resources/mqtt-0.1.jar"));
        return jobConfig;
    }

}
