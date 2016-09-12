How to run this example

1. Install cassandra
2. Install kafka
3. Install zeppeling with dcos package install --package-version=0.6.0 zeppelin
   * `dcos package install zeppelin --package-version=0.6.0`
   * Add the labels `HAPROXY_GROUP=external` and `HAPROXY_0_PORT=8085`
   * Change the cassandra interpreter settings hosts properies for `cassandra.hosts` to: `node-0.cassandra.mesos,node-1.cassandra.mesos,node-2.cassandra.mesos`
4. Import the Zepplin notebook `/Zeppelin-Notebooks/Sensor Data in Cassandra.json` as well as `Battery Dashboard.json` and create the keyspace and the tables
5. Start the sensor ingestion app
6. Build the fat jar with the gradle task `./gradlew fatJarForSparkSubmit`
7. Upload it to s3
8. Run the Spark job with: `dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.KafkaToCassandra https://s3-us-west-1.amazonaws.com/<your_bucket>/KafkaToCassandra-all.jar"`
9. Send Data to the sensor-ingestion app that looks like:
   `{
        "z" : -0.004197141877964879,
        "x" : -0.0617911962442365,
        "y" : 0.07009919358084769,
        "date" : "2016-09-03T08:40:25.150+02:00",
        "type" : "Gyro"
    }`
