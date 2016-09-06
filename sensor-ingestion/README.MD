# Steps to build and deploy Akka REST Service for Data Ingestion

This requires [Docker](https://www.docker.com/) and uses [DockerHub](https://hub.docker.com/) for convenience to publish images usable by [Marathon](https://docs.mesosphere.com/1.7/usage/tutorials/docker-app/).
You must have Docker setup and given permissions to your user in order for this to work.
See <https://github.com/Zuehlke/SHMACK/blob/master/Docker.md> for details!

## Regular deployment into a [SHMACK DC/OS Cluster](https://github.com/Zuehlke/SHMACK)

* `./gradlew distDocker --stacktrace` (or trigger this in your IDE of choice)
* `docker push mspringma1/sensor-ingestion:latest` to publish the updated image
* `dcos marathon app add sensor-ingestion-options.json` to deploy the application on DC/OS
* to be sure: `open-shmack-haproxy-stats.sh` and search for port of sensor-ingestion, should be 8083; copy hostname from URL.
* `~/shmack/repo/04_implementation/scripts/helpers/open-browser.sh  http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/hello` with a corrected hostname from the HAProxy stats provides a healthcheck
* `curl --data '{"sensor":"dummy", "data":"nothing"}' --request POST --basic --user hackzurich http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/sensorReading/curl`
  Will ask for password, then post some data, should return: **Sent msg to kafka topic sensor-reading with key  curl!**
* `~/shmack/repo/04_implementation/scripts/helpers/open-browser.sh  http://ec2-54-183-172-209.us-west-1.compute.amazonaws.com:8083/sensorReading/`
  Should tell you after login just in a single number how many messages have been processed since last restart
  
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

