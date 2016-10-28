Simulation of sensor-ingestion clients like [Sensor App](https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md)

```Usage: ClientSimulator <options> <service url>
when --data-basis is not provided, random data will be generated and sent
<options>
 --auth=user:password       uses basic auth provided
 --message-count=<count>            the amount of random data sent. (not effect on --data-basis)
 --data-basis=<folder/filer>            sends the kafka-captured data in the provided folder or file```