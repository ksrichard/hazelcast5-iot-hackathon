package com.hazelcast.hackathon;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.contrib.mqtt.MqttSources;
import com.hazelcast.jet.contrib.mqtt.Subscription;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.python.PythonServiceConfig;
import com.hazelcast.jet.python.PythonTransforms;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hivemq.embedded.EmbeddedHiveMQ;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.net.MalformedURLException;
import java.util.AbstractMap;
import java.util.Map;

public class Application {

    public static final String TEMPERATURE_MQTT_TOPIC_NAME = "temp";

    public static final String TEMPERATURE_MAP_NAME = "temp";

    public static final String TEMPERATURE_STATS_MAP_NAME = "temp_stats";

    public static final String TEMP_STREAM_STATS_AVERAGE_MAP_KEY = "temp_stream_avg";

    public static final String TEMP_STREAM_STATS_MIN_MAP_KEY = "temp_stream_min";

    public static final String TEMP_STREAM_STATS_MAX_MAP_KEY = "temp_stream_max";

    public static final String TEMP_MAP_STATS_AVERAGE_MAP_KEY = "temp_map_avg";

    public static final String TEMP_MAP_STATS_MIN_MAP_KEY = "temp_map_min";

    public static final String TEMP_MAP_STATS_MAX_MAP_KEY = "temp_map_max";

    public static final String AVG_TEMPERATURE_JOB_NAME = "average-temp";

    public static final String MIN_TEMPERATURE_JOB_NAME = "min-temp";

    public static final String MAX_TEMPERATURE_JOB_NAME = "max-temp";

    public static final String MQTT_INGESTION_JOB_NAME = "mqtt-ingestion";

    public static final String MODEL_TRAIN_JOB_NAME = "temp-model-train";

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
            if (event.getKey().equalsIgnoreCase(TEMP_MAP_STATS_AVERAGE_MAP_KEY)) {
                System.out.println("AVG map temperature: " + event.getValue());
            }
            if (event.getKey().equalsIgnoreCase(TEMP_MAP_STATS_MIN_MAP_KEY)) {
                System.out.println("MIN map temperature: " + event.getValue());
            }
            if (event.getKey().equalsIgnoreCase(TEMP_MAP_STATS_MAX_MAP_KEY)) {
                System.out.println("MAX map temperature: " + event.getValue());
            }
        }, true);

        // start MQTT
        EmbeddedHiveMQ hiveMQ = EmbeddedHiveMQ.builder().build();
        hiveMQ.start().join();

        // TODO: implement prediction/training pipelines
        initMapStatsPipelines(hzClient, temperaturesMap, tempStatsMap);
        startMqttIngestionPipeline(hzClient, temperaturesMap, tempStatsMap);
    }

    private static void initMapStatsPipelines(HazelcastInstance hzClient, IMap<Long, Double> temperatureMap, IMap<String, Double> tempStatsMap) {
        // average temperature pipeline
        Pipeline avgTempPipeline = Pipeline.create();
        avgTempPipeline.readFrom(Sources.map(temperatureMap))
                .aggregate(AggregateOperations.averagingDouble(Map.Entry::getValue))
                .map(value -> new AbstractMap.SimpleEntry<>(TEMP_MAP_STATS_AVERAGE_MAP_KEY, value))
                .writeTo(Sinks.map(tempStatsMap));

        // min temperature pipeline
        Pipeline minTempPipeline = Pipeline.create();
        minTempPipeline.readFrom(Sources.map(temperatureMap))
                .aggregate(AggregateOperations.minBy((o1, o2) -> o1.getValue().compareTo(o2.getValue())))
                .map(value -> new AbstractMap.SimpleEntry<>(TEMP_MAP_STATS_MIN_MAP_KEY, value.getValue()))
                .writeTo(Sinks.map(tempStatsMap));

        // max temperature pipeline
        Pipeline maxTempPipeline = Pipeline.create();
        maxTempPipeline.readFrom(Sources.map(temperatureMap))
                .aggregate(AggregateOperations.maxBy((o1, o2) -> o1.getValue().compareTo(o2.getValue())))
                .map(value -> new AbstractMap.SimpleEntry<>(TEMP_MAP_STATS_MAX_MAP_KEY, value.getValue()))
                .writeTo(Sinks.map(tempStatsMap));

        temperatureMap.addEntryListener((EntryAddedListener<Long, Double>) event -> {
            hzClient.getJet().newJob(avgTempPipeline, getJobConfig(AVG_TEMPERATURE_JOB_NAME)).join();
            hzClient.getJet().newJob(minTempPipeline, getJobConfig(MIN_TEMPERATURE_JOB_NAME)).join();
            hzClient.getJet().newJob(maxTempPipeline, getJobConfig(MAX_TEMPERATURE_JOB_NAME)).join();
        }, true);
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
        mqttIngestionJob = hzClient.getJet().newJob(mqttIngestionPipeline, jobConfig);
        mqttIngestionJob.join();
    }
    private static JobConfig getJobConfig(String jobName) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        jobConfig.addClass(Application.class);
        jobConfig.addJar(new File("src/main/resources/mqtt-0.1.jar"));
        return jobConfig;
    }

    private static void initPredictionPipeline(HazelcastInstance hzClient, IMap<Long, Double> temperatureMap) {
        Pipeline pipeline = Pipeline.create();
        FunctionEx<StreamStage<String>, StreamStage<String>> pythonStage = PythonTransforms.mapUsingPython(new PythonServiceConfig()
                .setBaseDir("python/train")
                .setHandlerModule("train")
                .setHandlerFunction("predict")
        );
        StageWithKeyAndWindow<Map.Entry<Long, Double>, Long> window = pipeline
                .readFrom(Sources.mapJournal(temperatureMap, JournalInitialPosition.START_FROM_CURRENT))
                .withTimestamps(Map.Entry::getKey, 10)
                .groupingKey(Map.Entry::getKey)
                .window(WindowDefinition.tumbling(5));
//        window.aggregate().writeTo(Sinks.logger());
//        window.writeTo(Sinks.files("test/"));
        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(Application.class);
        jobConfig.addClass(Application.class);
        jobConfig.addJar(new File("src/main/resources/mqtt-0.1.jar"));

        temperatureMap.addEntryListener((EntryAddedListener<Long, Double>) event -> {
            hzClient.getJet().newJob(pipeline, jobConfig).join();
        }, true);
    }

    private static void initTrainingPipeline(HazelcastInstance hzClient, IMap<Long, Double> temperatureMap) {
        // model training pipeline
        Pipeline trainingPipeline = Pipeline.create();
//        trainingPipeline
//                .readFrom(Sources.mapJournal(temperatureMap, JournalInitialPosition.START_FROM_CURRENT))
//                .withTimestamps(Map.Entry::getKey, 10)
//                .map(entry -> entry.getValue().toString())
//                .peek()
//                .apply(PythonTransforms.mapUsingPython(
//                        new PythonServiceConfig()
//                                .setBaseDir("python/train")
//                                .setHandlerModule("train")
//                                .setHandlerFunction("train_model")
//                        ))
//                .setLocalParallelism(1)
//                .peek()
//                .writeTo(Sinks.files("asd/"));


        trainingPipeline.readFrom(Sources.map(temperatureMap))
                .sort((o1, o2) -> o1.getKey().compareTo(o2.getKey()))
                .map(entry -> entry.getValue().toString())
                .apply(PythonTransforms.mapUsingPythonBatch(
                        new PythonServiceConfig()
                                .setBaseDir("python/train")
                                .setHandlerModule("train")
                                .setHandlerFunction("train_model")
                        )
                )
                .writeTo(Sinks.files("asd/"));
        JobConfig trainingJobConfig = new JobConfig();
        trainingJobConfig.addClass(Application.class);
        trainingJobConfig.setName(MODEL_TRAIN_JOB_NAME);
        trainingJobConfig.addClass(Application.class);
        trainingJobConfig.addJar(new File("src/main/resources/mqtt-0.1.jar"));

        Job modelTrainJob = hzClient.getJet().getJob(MODEL_TRAIN_JOB_NAME);
        if (modelTrainJob != null) {
            modelTrainJob.cancel();
        }

        // map listener
        //TODO: trigger model training at every 100 or 1000 element addition
        temperatureMap.addEntryListener((EntryAddedListener<Long, Double>) event -> {
            // TODO: with job name set it produces an exception after first run,
            // TODO: and w/o a name we create a lot of jobs on Jet which should be cleaned up somehow
            hzClient.getJet().newJob(trainingPipeline, trainingJobConfig).join();
        }, true);
    }

}
