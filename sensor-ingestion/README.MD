Steps to build and deploy Akka REST Service for Data Ingestion

This requires [Docker](https://www.docker.com/) and uses [DockerHub](https://hub.docker.com/) for convenience to publish images usable by [Marathon](https://docs.mesosphere.com/1.7/usage/tutorials/docker-app/).
You must have Docker setup and given permissions to your user in order for this to work.
See <https://github.com/Zuehlke/SHMACK/blob/master/Docker.md> for details!

- ./gradlew distDocker --stacktrace
- docker push mspringma1/sensor-ingestion:latest
- dcos marathon app add sensor-ingestion-options.json
- to be sure: open-shmack-haproxy-stats.sh and search for port of sensor-ingestion, should be 8083; copy hostname from URL.
- ~/shmack/repo/04_implementation/scripts/helpers/open-browser.sh  http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/hello
- curl --data '{"sensor":"dummy", "data":"nothing"}' --request POST --basic --user hackzurich http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/sensorReading/curl
  Will ask for password, should return: Sent msg to kafka topic curl!
- ~/shmack/repo/04_implementation/scripts/helpers/open-browser.sh  http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/sensorReading/
  Should tell you just in a single number how many messages have been processed since last restart


