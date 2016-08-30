To test the KafkaToS3 Streaming job do:

1. Start the Mesos cluster
2. Install Spark and Kafka
3. Add a topic
4. Create a fat jar with gradle
5. Upload the fat jar to a place accessible from the cluster e.g. S3
6. Submit the spark job with:

dcos spark run --submit-args="--supervise --conf spark.mesos.uris=http://hdfs.marathon.mesos:9000/v1/connect/hdfs-site.xml,http://hdfs.marathon.mesos:9000/v1/connect/core-site.xml --class com.zuehlke.hackzurich.KafkaToS3 https://s3-us-west-1.amazonaws.com/zuehlkesensordata/kafka-s3-backup-all.jar <kafkaBrokerInclPort> <topicName> <bucketName> <awsId> >awsSecret>"
