package com.hazelcast.hackathon;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

import static com.hazelcast.hackathon.Application.TEMPERATURE_MAP_NAME;
import static com.hazelcast.hackathon.Application.TEMPERATURE_STATS_MAP_NAME;
import static com.hazelcast.hackathon.Application.TEMP_STREAM_STATS_AVERAGE_MAP_KEY;
import static com.hazelcast.hackathon.Application.TEMP_STREAM_STATS_MAX_MAP_KEY;
import static com.hazelcast.hackathon.Application.TEMP_STREAM_STATS_MIN_MAP_KEY;

@ServerEndpoint("/sensors/{clientName}")
@ApplicationScoped
public class DashboardSocket {

    Map<String, Session> sessions = new ConcurrentHashMap<>();

    HazelcastInstance hzClient = HazelcastClient.newHazelcastClient();

    ObjectMapper mapper = new ObjectMapper();

    @OnOpen
    public void onOpen(Session session, @PathParam("clientName") String clientName) {
        sessions.put(clientName, session);
        IMap<Long, Double> temperatureMap = hzClient.getMap(TEMPERATURE_MAP_NAME);
        IMap<String, Double> statsMap = hzClient.getMap(TEMPERATURE_STATS_MAP_NAME);
        temperatureMap.addEntryListener((EntryAddedListener<Long, Double>) event -> {
            StatResponse statResponse = new StatResponse(
                event.getValue(),
                statsMap.get(TEMP_STREAM_STATS_AVERAGE_MAP_KEY),
                statsMap.get(TEMP_STREAM_STATS_MIN_MAP_KEY),
                statsMap.get(TEMP_STREAM_STATS_MAX_MAP_KEY)
            );
            try {
                broadcast(mapper.writeValueAsString(statResponse));
            }
            catch (JsonProcessingException e) {
                System.out.println("Serialization error: " + e);
            }
        }, true);
        System.out.println("Session " + clientName + " joined");
    }

    @OnClose
    public void onClose(Session session, @PathParam("clientName") String clientName) {
        sessions.remove(clientName);
        System.out.println("Session " + clientName + " closed");
    }

    @OnError
    public void onError(Session session, @PathParam("clientName") String clientName, Throwable throwable) {
        sessions.remove(clientName);
        System.out.println("Sensor session " + clientName + " closed on error: " + throwable);
    }

    @OnMessage
    public void onMessage(String message, @PathParam("clientName") String clientName) {
        System.out.println(">> " + clientName + ": " + message);
    }

    private void broadcast(String message) {
        sessions.values().forEach(s -> {
            s.getAsyncRemote().sendObject(message, result ->  {
                if (result.getException() != null) {
                    System.out.println("Unable to send message: " + result.getException());
                }
            });
        });
    }
}
