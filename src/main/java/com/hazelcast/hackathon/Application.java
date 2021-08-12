package com.hazelcast.hackathon;
import com.codahale.metrics.MetricRegistryListener;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.aggregate.AggregateOperation1Impl;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.python.PythonServiceConfig;
import com.hazelcast.jet.python.PythonTransforms;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapListenerAdapter;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.MapListener;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.Mqtt5Message;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.embedded.EmbeddedHiveMQ;
import com.hivemq.embedded.EmbeddedHiveMQBuilder;
import com.hivemq.metrics.MetricRegistryLogger;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Tensor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Application {

    public static void main(String[] args) throws InterruptedException {
        // start Hz client
        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient();
        IMap<Long, Double> temperaturesMap = hzClient.getMap("temp");

        // TODO: check if training should be put in a Jet pipeline
        // model training pipeline
        Pipeline trainingPipeline = Pipeline.create();
        trainingPipeline.readFrom(Sources.map(temperaturesMap))
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
        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(Application.class);
        jobConfig.addClass(Mqtt5Publish.class);
        jobConfig.addClass(Mqtt5Message.class);

        // TODO: implement prediction pipeline


        // map listener
        //TODO: trigger model training at every 100 or 1000 element addition
        temperaturesMap.addEntryListener((EntryAddedListener<Long, Double>) event -> {
            hzClient.getJet().newJob(trainingPipeline, jobConfig).join();
        }, true);

        // start MQTT
        EmbeddedHiveMQ hiveMQ = EmbeddedHiveMQ.builder().build();
        hiveMQ.start().join();

        // TODO: check MQTT connector as source for pipelines instead of manually starting MQTT client and put data to it
        // start mqtt client
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .serverHost("0.0.0.0")
                .serverPort(1883)
                .automaticReconnect()
                .applyAutomaticReconnect()
                .build().toBlocking();
        client.connect();

        Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        client.subscribeWith().topicFilter("temp").qos(MqttQos.AT_LEAST_ONCE).send();
        while(true) {
            publishes.receive(0, TimeUnit.MICROSECONDS).ifPresent(mqtt5Publish -> {
                String payload = new String(mqtt5Publish.getPayloadAsBytes());
                System.out.println("Current temp: " + payload);
                temperaturesMap.put(System.currentTimeMillis(), Double.parseDouble(payload));
            });
        }
    }

}
