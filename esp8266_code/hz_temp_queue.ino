#include <Arduino.h>
#include <ESP8266WiFi.h>
#include <ESP8266mDNS.h>
#include <WiFiUdp.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <PubSubClient.h>

// WiFi
const char* ssid     = "WIFI_SSID";
const char* password = "WIFI_PASSWORD";

// OneWire GPIO
const int oneWireBus = 4;
OneWire oneWire(oneWireBus);

// Temperature
DallasTemperature sensors(&oneWire);

// MQTT
const char* mqttServer = "MQTT_SERVER_ADDRESS";
WiFiClient espClient;
PubSubClient client(espClient);
#define MSG_BUFFER_SIZE	(50)
char msg[MSG_BUFFER_SIZE];

#define LOG_D(fmt, ...)   printf_P(PSTR(fmt "\n") , ##__VA_ARGS__);

void setup() {
  Serial.begin(9600);

    //   connect to WIFI
    WiFi.begin ( ssid, password );
    Serial.println ("Connecting to WIFI");
  
    // Wait for connection
    while ( WiFi.status() != WL_CONNECTED ) {
      delay ( 500 );
      Serial.print ( "." );
    }
  
    Serial.println ( "" );
    Serial.print ( "Connected to " );
    Serial.println ( ssid );
    Serial.print ( "IP address: " );
    Serial.println ( WiFi.localIP() );

   // Start the DS18B20 sensor
   sensors.begin();
   Serial.println ( "Temperature sensor setup done" );

   // MQTT
  client.setServer(mqttServer, 1883);
}

void reconnect() {
  // Loop until we're reconnected
  while (!client.connected()) {
    Serial.println("[MQTT] Attempting MQTT connection...");
    // Create a random client ID
    String clientId = "NodeMcuClient-";
    clientId += String(random(0xffff), HEX);
    // Attempt to connect
    if (client.connect(clientId.c_str())) {
      Serial.println("[MQTT] Connected");
    } else {
      Serial.print("[MQTT] failed, rc=");
      Serial.print(client.state());
      Serial.println(" try again in 5 seconds");
      // Wait 5 seconds before retrying
      delay(5000);
    }
  }
}

void loop() {
  if (!client.connected()) {
    reconnect();
  }
  client.loop();

  sensors.requestTemperatures();
  float temperatureC = sensors.getTempCByIndex(0);
  LOG_D("Current temperature: %.3f ºC", temperatureC);

  snprintf(msg, MSG_BUFFER_SIZE, "%.3f", temperatureC);
  client.publish("temp", msg);

  delay(100);
}
