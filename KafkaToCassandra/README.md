How to run this example

1. Make sure [Kafka](https://docs.mesosphere.com/1.7/usage/service-guides/kafka/), [Cassandra](https://docs.mesosphere.com/1.7/usage/service-guides/cassandra/) and [Marathon-LB]() are installed
   * `dcos package install kafka`
   * `dcos package install cassandra`
   * `dcos package install marathon-lb`
2. Install Zeppelin with in a newer version than the default
   * `dcos package install zeppelin --package-version=0.6.0`
   * Make sure Zeppelin runs `open-shmack-zeppelin.sh` (if redirected to login, you may have to execute the command a second time after login)
   * Change the Cassandra interpreter settings hosts properties for `cassandra.hosts` to: `node-0.cassandra.mesos,node-1.cassandra.mesos,node-2.cassandra.mesos`
3. Use Zeppelin notebooks to create the required Cassandra tables 
   * Import the Zepplin notebook [Zeppelin-Notebooks/Sensor Data in Cassandra.json](https://www.zeppelinhub.com/viewer/notebooks/aHR0cHM6Ly9yYXcuZ2l0aHVidXNlcmNvbnRlbnQuY29tL1p1ZWhsa2UvaGFja3p1cmljaC1zZW5zb3JkYXRhYW5hbHlzaXMvbWFzdGVyL0thZmthVG9DYXNzYW5kcmEvWmVwcGVsaW4tTm90ZWJvb2tzL1NlbnNvciUyMERhdGElMjBpbiUyMENhc3NhbmRyYS5qc29u)
   * as well as [Battery Dashboard.json](https://www.zeppelinhub.com/viewer/notebooks/aHR0cHM6Ly9yYXcuZ2l0aHVidXNlcmNvbnRlbnQuY29tL1p1ZWhsa2UvaGFja3p1cmljaC1zZW5zb3JkYXRhYW5hbHlzaXMvbWFzdGVyL0thZmthVG9DYXNzYW5kcmEvWmVwcGVsaW4tTm90ZWJvb2tzL0JhdHRlcnklMjBEYXNoYm9hcmQuanNvbg) 
   * Run the paragraphs to create the keyspace and the tables
4. Make sure the [Sensor Ingestion Akka REST Service](https://github.com/Zuehlke/hackzurich-sensordataanalysis/tree/master/sensor-ingestion) is running
5. Build the fat jar with the gradle task `./gradlew fatJarForSparkSubmit`
6. Upload it to a URL that can get reached from the cluster, e.g. [Amazon S3 following these instructions](https://github.com/Zuehlke/hackzurich-sensordataanalysis/blob/master/S3ForSparkSubmit.md) 
8. Run the Spark job with: `dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.KafkaToCassandra https://s3-us-west-1.amazonaws.com/<your_bucket>/KafkaToCassandra-all.jar"`
   HINT: For error tracking, may additionaly use the option `--total-executor-cores 1` to run the job just on one node if contuous data stream can get handled by a single node,
   thus leaving more "room" in your cluster for other jobs running concurrently
9. Send Data to the sensor-ingestion app that looks like:
   `{
        "z" : -0.004197141877964879,
        "x" : -0.0617911962442365,
        "y" : 0.07009919358084769,
        "date" : "2016-09-03T08:40:25.150+02:00",
        "type" : "Gyro"
    }`
