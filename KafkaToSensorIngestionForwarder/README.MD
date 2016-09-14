Provides a replacement for [Kafka MirrorMaker](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330) <https://kafka.apache.org/documentation.html#basic_ops_mirror_maker> 
that does not require to build tunnels / punch holes into the security of DC/OS in order to propagate messages from one Kafka cluster to another.

This is achieved by exploiting the knowledge of the Sensor Ingestion Akka REST Service.

1. Create a fat jar containing all dependencies defined in Gradle with `./gradlew fatJar`
2. Upload the fat jar to a place accessible from the cluster e.g. S3
3. Submit the spark job with:

`dcos spark run --submit-args="--supervise --total-executor-cores 1 --class com.zuehlke.hackzurich.KafkaToSensorIngestionForwarder <jar_location> <topics> <sensor-ingestion-endpoint> <user> <password> <batchInterval> <startPosition>"`

Hint for streaming jobs: 
`--supervise` will let Spark restart the job whenever it failed - for instance, when endpoint was temporarily unavailable.

`--total-executor-cores 1` will limit the execution to a single core on a single node, thus not using any parallelism.