How to run this example

1. Install cassandra
2. Install kafka
3. Install zeppeling with dcos package install --package-version=0.6.0 zeppelin

3.0 dcos package install zeppelin --package-version=0.6.0
3.1 Add the labels HAPROXY_GROUP=external and HAPROXY_0_PORT=8085
3.2 Change the cassandra interpreter settings hosts properies to: node-0.cassandra.mesos

4. Import the Zepplin notebook /Zeppelin-Notebooks/Cassandra Setup Gyro-Sensor.json and create the keyspace and the table
5. Start the sensor ingestion app
6. Build the fat jar with the gradle task fatJar
7. Upload it to s3
8. Run the Spark job with: dcos spark run --submit-args="--supervise --conf spark.mesos.uris=http://hdfs.marathon.mesos:9000/v1/connect/hdfs-site.xml,http://hdfs.marathon.mesos:9000/v1/connect/core-site.xml --class com.zuehlke.hackzurich.KafkaToCassandra https://s3-us-west-1.amazonaws.com/your_bucket/KafkaToCassandra-all.jar sensor-reading hackzurichzuhlke gyrodata"
9. Send Data to the sensor-ingestion app that looks like:
{
    "z" : -0.004197141877964879,
    "x" : -0.0617911962442365,
    "y" : 0.07009919358084769,
    "date" : "2016-09-03T08:40:25.150+02:00",
    "type" : "Gyro"
  }
