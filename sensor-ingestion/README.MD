Steps to build and deploy Akka REST Service for Data Ingestion

- sudo ../gradlew distDocker --stacktrace
- sudo docker push mspringma1/sensor-ingestion:latest
- dcos marathon app add sensor-ingestion-options.json
- to be sure: open-shmack-haproxy-stats.sh and search for port of sensor-ingestion, should be 8083; copy hostname from URL.
- ~/shmack/repo/04_implementation/scripts/helpers/open-browser.sh  http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/hello
- curl --data '{"sensor":"dummy", "data":"nothing"}' --request POST  http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/sensorReading/curl
  Should return:  <h1>Sent msg to kafka topic curl!</h1>
-

