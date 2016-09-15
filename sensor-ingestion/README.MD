# Steps to build and deploy Akka REST Service for Data Ingestion

This requires [Docker](https://www.docker.com/) and uses [DockerHub](https://hub.docker.com/) for convenience to publish images usable by [Marathon](https://docs.mesosphere.com/1.7/usage/tutorials/docker-app/).
You must have Docker setup and given permissions to your user in order for this to work.
See <https://github.com/Zuehlke/SHMACK/blob/master/Docker.md> for details!

## Regular deployment into a [SHMACK DC/OS Cluster](https://github.com/Zuehlke/SHMACK)

* Preparing Kafka:
  * Make sure Kafka is ready to use on your cluster, so at least `dcos package install kafka` must be finished
  * Optional: Create the topic to use for ingestion with multiple partitions, so assignment of partitions to brokers is possible: `dcos kafka topic create sensor-reading --partitions 20 --replication 3`
  * Optional: Check it is configured as desired: `dcos kafka topic describe sensor-reading`
* Optional: Change username and password in [resources/application.conf](https://github.com/Zuehlke/hackzurich-sensordataanalysis/blob/master/sensor-ingestion/src/main/resources/application.conf) 
* Build the app with `./gradlew distDocker --stacktrace` (or trigger this in your IDE of choice)
* Publish the updated image with `docker push <your_dockerhub_user>/sensor-ingestion:latest` 
* Finally, deploy the application on DC/OS with `dcos marathon app add sensor-ingestion-options.json` 
* Optional: Check if everything works
  * `open-shmack-haproxy-stats.sh` and search for port of sensor-ingestion, should be 8083; copy hostname from URL.
  * `~/shmack/repo/04_implementation/scripts/helpers/open-browser.sh  <hostname:port>/hello` with a corrected hostname from the HAProxy stats provides a healthcheck
  * `curl --data '"{"z" : -0.1, "x" : -0.2, "y" : 0.1, "date" : "2016-09-03T08:40:17.552+02:00", "type" : "Gyro"}"' --request POST --basic --user hackzurich <hostname:port>/sensorReading/curl`
    will ask for password, then post some data, should return: **Sent msg to kafka topic sensor-reading with key  curl!**
  * `~/shmack/repo/04_implementation/scripts/helpers/open-browser.sh  http://<hostname:port>/sensorReading/`
    should tell you after login just in a single number how many messages have been processed since last restart
* Optional: To monitor incoming data, most basic but very reliable way is:
   * `ssh-into-dcos-master.sh`
   * `docker run -it mesosphere/kafka-client`
   * `./kafka-console-consumer.sh --zookeeper master.mesos:2181/dcos-service-kafka --topic sensor-reading`
* Optional: Install Zeppelin and import [zeppelin-notebooks/Check Sensor Data Ingestion.json](https://www.zeppelinhub.com/viewer/notebooks/aHR0cHM6Ly9yYXcuZ2l0aHVidXNlcmNvbnRlbnQuY29tL1p1ZWhsa2UvaGFja3p1cmljaC1zZW5zb3JkYXRhYW5hbHlzaXMvbWFzdGVyL3NlbnNvci1pbmdlc3Rpb24vemVwcGVsaW4tbm90ZWJvb2tzL0NoZWNrJTIwU2Vuc29yJTIwRGF0YSUyMEluZ2VzdGlvbi5qc29u) 
  following these insctructions.
## Running a local server that allows testing the endpoint
* Check out the repository as usual
* `./gradlew runLocalTestServer`
  Will show `Server online at http://localhost:18080/` and block with something like `> Building 80% > :runLocalTestServer`
* Open <http://localhost:18080/hello>
* In a new shell `curl --data '{"sensor":"dummy", "data":"nothing"}' --request POST --basic --user hackzurich http://localhost:18080/sensorReading/curl`
  Will ask for password, then post some data, should return: **Sent msg to kafka topic sensor-reading with key curl!**
* Return to shell where you launched `./gradlew runLocalTestServer` and see that the message appeared.
* Open <http://localhost:18080/sensorReading/> as user `hackzurich` to see the count
* Code against this local endpoint. 
  * By default, this script will start a server that only listens on localhost. 
  * If you need to make the server also available remotely, end it with `[Ctrl]-[C]` on shell,
  * `export HOSTNAME="<network reachable hostname>"` with your name or ip-address your server will be reachable,
  * restart with `./gradlew runLocalTestServer`

## Additional Setup of main ingestion node for the Event
* For the stack creation using [SHMACK](https://github.com/Zuehlke/SHMACK)
  * Adjust in `shmack_env` 
    STACK_NAME="HackZurich-MainSensorIngestionStack"
    TEMPLATE_URL="https://s3-us-west-1.amazonaws.com/shmack/single-master.cloudformation.json"
  * Adjust in `create-stack.sh` 
    SLAVE_INSTANCE_COUNT=8 
    SLAVE_INSTANCE_TYPE="r3.xlarge"
    INSTALL_PACKAGES="spark marathon marathon-lb hdfs kafka cassandra"
  * Optional: Increase number of public slaves to provide failover capabilty and load balancing
  * Execute `create-stack.sh` to create cluster on AWS
  * Install Zeppelin in version 0.6.0: `dcos package install --package-version=0.6.0 zeppelin`
* `open-shmack-marathon-ui.sh` and edit configuration of Kafka to modify environment variables that define broker config and defaults
  * Set retention to approximately 1 month (30 days = 720 hours): `"KAFKA_OVERRIDE_LOG_RETENTION_HOURS": "720",`
  * Increase a lot the disc allocation per broker, here 50 GB instead of just 5 GB: `"BROKER_DISK": "50000",`
  * Use more available nodes as brokers: `"BROKER_COUNT": "5",`
  * Optional: Turn on replication for topics by default: `"KAFKA_OVERRIDE_DEFAULT_REPLICATION_FACTOR": "3",`
  * Optional: Require in-sync replicas: `"KAFKA_OVERRIDE_MIN_INSYNC_REPLICAS": "2",`
  * Activate this new default config by clicking button Change and Deploy Configuration. 
    Note that this will only redeploy the Kafka Mesus Scheduler to adjust the broker configuration. 
    The brokers itself run inside the Kafka Mesos Executor as described in <https://mesosphere.com/blog/2015/07/16/making-apache-kafka-elastic-with-apache-mesos>, 
    which also ensures that changing values in Marathon will not let you loose data stored in Kafka so far.
* Create the topic and ensure its configuration
  * Create the topic to use for ingestion with multiple partitions, so assignment of partitions to brokers is possible: `dcos kafka topic create sensor-reading --partitions 20 --replication 3`
  * Check it is configured as desired: `dcos kafka topic describe sensor-reading`
* Now basically do the regular deployment through Docker/Marathon that will use the already created topic, but:
  * In sensor-ingestion-options.json, set `"HAPROXY_0_PORT": "80"`, so the server will be accessible on default port 80, 
    for which DC/OS creates an [Elastic Load Balancer (ELB)](https://us-west-1.console.aws.amazon.com/ec2/v2/home?region=us-west-1#LoadBalancers:) by default (together with one to access the admin interface)
* Optional: When missing some points at the time Kafka is deployed and topics are created, many of the parameters can get fixed later.
  Some information stated here because there were some changes in the commands in Kafka 0.10, so a lot of the examples you find on [StackOverflow](http://stackoverflow.com/questions/29129222/changing-kafka-rentention-period-during-runtime) etc. don't work anymore exactly as posted.
  * Connect to the cluster directly: `ssh-into-dcos-master.sh`
  * Get the current version of the Kafka command-line tooling: `docker run -it mesosphere/kafka-client`
  * Low-level topic list: `./kafka-topics.sh --list --zookeeper master.mesos:2181/dcos-service-kafka`
  * Describe the topic, showing changes from default config: `./kafka-topics.sh --describe --topic sensor-reading --zookeeper master.mesos:2181/dcos-service-kafka`
  * Changing a parameter -here retention to 100 days- of a topic: `./kafka-configs.sh --alter --add-config retention.ms=8640000000 --entity-type topics --entity-name sensor-reading --zookeeper master.mesos:2181/dcos-service-kafka` 

* Further information:
  * [DC/OS Kafka Documentation](https://docs.mesosphere.com/1.7/usage/service-guides/kafka/)
  * [DC/OS Kafka Service and CLI](https://github.com/mesosphere/dcos-kafka-service)
  * [Limitations](https://docs.mesosphere.com/1.7/usage/service-guides/kafka/limitations/)
  * [DC/OS Kafka Tutorial](https://dcos.io/docs/1.7/usage/tutorials/kafka/)
  * [Kafka on Mesos](https://github.com/mesos/kafka)
  * [Why not run Kafka on Marathon? Making Apache Kafka Elastic With Apache Mesos](https://mesosphere.com/blog/2015/07/16/making-apache-kafka-elastic-with-apache-mesos/)
  * [Kafka Operations Documentation](http://kafka.apache.org/documentation.html#basic_ops)