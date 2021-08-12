package com.hazelcast.hackathon;

import java.io.File;

import com.hivemq.embedded.EmbeddedHiveMQ;
import org.apache.commons.lang3.math.NumberUtils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.contrib.mqtt.MqttSources;
import com.hazelcast.jet.contrib.mqtt.Subscription;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.python.PythonServiceConfig;
import com.hazelcast.jet.python.PythonTransforms;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

public class Application {

    public static final String TEMPERATURE_MAP_NAME = "temp";

    public static final String MQTT_INGESTION_JOB_NAME = "mqtt-ingestion";

    public static final String MODEL_TRAIN_JOB_NAME = "temp-model-train";

    public static void main(String[] args) {
        // start Hz client
        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient();

        IMap<Long, Double> temperaturesMap = hzClient.getMap(TEMPERATURE_MAP_NAME);

        // TODO: check if training should be put in a Jet pipeline

        // start MQTT
        EmbeddedHiveMQ hiveMQ = EmbeddedHiveMQ.builder().build();
        hiveMQ.start().join();

        // TODO: implement prediction pipeline
        initTrainingPipeline(hzClient, temperaturesMap);
        startMqttIngestionPipeline(hzClient, temperaturesMap);
    }

    private static void initTrainingPipeline(HazelcastInstance hzClient, IMap<Long, Double> temperatureMap) {
        // model training pipeline
        Pipeline trainingPipeline = Pipeline.create();
        trainingPipeline.readFrom(Sources.map(temperatureMap))
            .sort((o1, o2) -> o1.getKey().compareTo(o2.getKey()))
            .map(entry -> entry.getValue().toString())
            .apply(PythonTransforms.mapUsingPythonBatch(
                new PythonServiceConfig()
                    .setBaseDir("python/train")
                    .setHandlerModule("train")
                )
            )
            .setLocalParallelism(1)
            .writeTo(Sinks.logger());
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

    private static void startMqttIngestionPipeline(HazelcastInstance hzClient, IMap<Long, Double> temperatureMap) {
        StreamSource<String> tempSource = MqttSources.builder()
            .clientId("jet-consumer")
            .broker("tcp://127.0.0.1:1883")
//            .auth("username", "password".toCharArray())
            .topic("temp")
            .qualityOfService(Subscription.QualityOfService.AT_LEAST_ONCE)
            .autoReconnect()
            .keepSession()
            .mapToItemFn((topic, message) -> new String(message.getPayload()))
            .build();

        // MQTT processing pipeline
        Pipeline mqttIngestionPipeline = Pipeline.create();
        mqttIngestionPipeline.readFrom(tempSource)
            .withoutTimestamps()
            .filter(NumberUtils::isCreatable)
            .map(Double::parseDouble)
            .peek()
            .setLocalParallelism(1)
            .writeTo(Sinks.mapWithUpdating(
                temperatureMap,
                aDouble -> System.currentTimeMillis(),
                (oldValue, newValue) -> newValue));

        JobConfig jobConfig = new JobConfig();

        jobConfig.setName(MQTT_INGESTION_JOB_NAME);
        jobConfig.addClass(Application.class);
        jobConfig.addJar(new File("src/main/resources/mqtt-0.1.jar"));
        Job mqttIngestionJob = hzClient.getJet().getJob(MQTT_INGESTION_JOB_NAME);
        if (mqttIngestionJob != null) {
            mqttIngestionJob.cancel();
        }
        mqttIngestionJob = hzClient.getJet().newJob(mqttIngestionPipeline, jobConfig);
        mqttIngestionJob.join();
    }

}
