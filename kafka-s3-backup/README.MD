To test the KafkaToS3 Streaming job do:

1. Start the Mesos cluster
2. Install Spark and Kafka ... and HDFS, since its tools provide basic S3 support
3. Add a topic
4. Create a fat jar containing all dependencies defined in Gradle with `./gradlew fatJarForSparkSubmit`
5. Upload the fat jar to a place accessible from the cluster e.g. [using S3](https://github.com/Zuehlke/hackzurich-sensordataanalysis/blob/master/S3ForSparkSubmit.md)
6. Create an [S3 bucket](https://console.aws.amazon.com/s3/home?region=us-west-1#) to store the backups and copy its URL 
6. Submit the spark job with:

`dcos spark run --submit-args="--supervise --total-executor-cores 1 --conf spark.mesos.uris=http://hdfs.marathon.mesos:9000/v1/connect/hdfs-site.xml,http://hdfs.marathon.mesos:9000/v1/connect/core-site.xml --class com.zuehlke.hackzurich.KafkaToS3 jar-location topicName bucketName awsId awsSecret"`

Hint for streaming jobs: 
`--supervise` will let Spark restart the job whenever it failed.

`--total-executor-cores 1` will limit the execution to a single core on a single node, thus not using any parallelism. 
While in general Spark Streaming could very well benefit from running on multiple nodes, the use of resources in Spark using default parameters may be excessive. 
And for a periodic backup task in background, it may actually be fine to use as little resources as possible to not block any other job.