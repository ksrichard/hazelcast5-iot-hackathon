IOT Weather Station using Hazelcast 5
---

This project aims to show how Hazelcast 5 can be used as a server to process 
temperature sensor data on the fly using Jet.

Usage
---
Required JDK version is **11**!
1. Download Hazelcast 5
   
   ``$ ./download-hz.sh``
2. Start Hazelcast 5

   ``$ ./start-hz-cluster.sh``

3. Package the application and run

    ``$ mvn clean package && java -jar target/iot-1.0-SNAPSHOT-jar-with-dependencies.jar``


