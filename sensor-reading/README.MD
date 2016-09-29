# Provides a simple Akka service to read the latest sensor-readings back from Kafka

Very similar to [Sensor Ingestion Akka REST Service](https://github.com/Zuehlke/hackzurich-sensordataanalysis/tree/master/sensor-ingestion#regular-deployment-into-a-shmack-dcos-cluster)
just reading from Kafka instead of writing.

Would be nice to provide a [WebSocket](http://doc.akka.io/docs/akka/2.4.10/scala/http/routing-dsl/websocket-support.html) such that 
clients can read indirectly the Kafka stream - but for now, they have to poll to get the most recent data from the service 
with no guarantee that they received all data (no gaps) or didn't see the same data multiple times.

For now, it is mainly a tool for debugging - to get relatively easy a feedback 
whether data send to the Sensor Ingestion REST Service really gets stored properly in Kafka.