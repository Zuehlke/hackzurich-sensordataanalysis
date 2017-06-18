Sensor Data Analysis for [HackZurich](http://hackzurich.com/)

Based on [SHMACK](https://github.com/Zuehlke/SHMACK) deploy the components to perform sensor data ingestion and analysis in a [DC/OS cluster](https://mesosphere.com/product/) in the [Amazon AWS Cloud](https://aws.amazon.com/).
The data can get received from the corresponding [smartphone apps for iOS](https://github.com/Zuehlke/hackzurich-sensordata-ios) or [Android](https://github.com/Zuehlke/hackzurich-sensordata-android)

# What kind of sensor data?

So far, we are expecting and prepared to receive the different data described here: <https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md>


# Getting started

In order to be able to run the sensor data analysis in the cloud, you need a running cluster.

*For HackZurich*: Please form teams and share a cluster. We will provide you with credentials each team can use to start up your cluster.
Each team should decide on a team name - so we can identify the credentials and resources asigned to you.
We will also assist you in the process of getting your environment ready to work, so don't hesitate to ask us for help.
Most likely you will avoid plenty of problems of you run SHMACK in an Ubuntu 16.04 VM and
use [IntelliJ IDEA](https://github.com/Zuehlke/SHMACK#optional-use-intellij-idea-for-shmack) as IDE instead of Eclipse
as it still has an edge over Eclipse with ScalaIDE when it comes to supporting language features of Scala and integrating Gradle.

Setting up a DC/OS cluster for this purpose works best using [SHMACK](https://github.com/Zuehlke/SHMACK) with the following settings:
* Stick to DC/OS 1.7; the newer version 1.8 has been released September 15 2016 ... it is bleeding edge and may bleed a little too much for now :-)
  By using `TEMPLATE_URL="https://s3-us-west-1.amazonaws.com/shmack/single-master.cloudformation.json"` in `shmack_env` you will use DC/OS 1.7.
  * Since Spark proposes In Memory computation and Cassandra/Kafka/HDFS provide their own redundancy,
    better switch to memory-optimized EC2 `SLAVE_INSTANCE_TYPE` in `shmack_env`, e.g. [r3.xlarge](https://aws.amazon.com/ec2/pricing/).
    They are a bit more expensive, but significantly increase the data you can handle per node compared to the default (m3.xlarge).
    Spot instances are always a good choice if you want to save money and don't care if you may loose part of or your entire cluster if you get outbid.
  * You may add all the packages you will need anyway in `create-stack.sh`: `INSTALL_PACKAGES="spark cassandra kafka marathon marathon-lb"`,
    so they already get installed when you create your cluster using the CloudFormation template.
  * *For HackZurich*: Set `STACK_NAME` in `shmack_env` to your team's name.
* Now you can go through the steps described in <https://github.com/Zuehlke/SHMACK>
  from [Installation](https://github.com/Zuehlke/SHMACK#installation) to [Stack Creation](https://github.com/Zuehlke/SHMACK#stack-creation-from-now-on-you-pay-for-usage).
* Once the stack is up-and-running an you see on the DC/OS master dashboard that services are healthy,
  you can deploy the [Sensor Ingestion Akka REST Service](https://github.com/Zuehlke/hackzurich-sensordataanalysis/tree/master/sensor-ingestion#regular-deployment-into-a-shmack-dcos-cluster)
  * *For HackZurich*: Just to get started, you don't need to build and push the application to docker yourself,
    you can just use the docker image from `bwedenik/sensor-ingestion` as defined in `sensor-ingestion-options.json`.
    Only after you make changes in the ingestion service, you will need to build and deploy your own docker images.
  * *For HackZurich*: When you have assured that in principle Sensor Ingestion is running properly, tell us your endpoint URL,
    so we can forward you the sensor readings we receive at the main ingestion cluster.
    You may use this -your own endpoint- with the iOS or Android app if you don't need other teams to see your sensor readings.
* Run the [KafkaToCassandra Spark streaming](https://github.com/Zuehlke/hackzurich-sensordataanalysis/tree/master/KafkaToCassandra) job,
  in the process of which Cassandra tables will get created to make it much nicer to query and visulize data in Zeppelin.
* Now you are basically done - you have a cluster running Mesos,
  an reactive Akka Web Service to store incoming data to Kafka,
  a Spark Streaming job reading from Kafka and writing to Cassandra,
  and Zeppelin to interactively explore the recorded data.
  In addition, you find some more examples in this repository,
  e.g. a [Simple Spark Testrun](https://github.com/Zuehlke/hackzurich-sensordataanalysis/tree/master/simple-spark-testrun) that can be a basis for your own jobs
  and [KafkaS3Backup](https://github.com/Zuehlke/hackzurich-sensordataanalysis/tree/master/kafka-s3-backup) that illustrates how to access external resources - in particular of AWS.
* *For HackZurich*: You may now focus on the challenge, good luck!



# Affiliation within Zühlke
Initiated for the purpose of [HackZurich](http://hackzurich.com/), main contributions from members of:

* Zühlke Focusgroup - Big Data / Cloud
* Zühlke Focusgroup - Data Analytics
* Zühlke Focusgroup - Reactive Computing

<a name="license"></a>
# License Details

Copyright 2016

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
