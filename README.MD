Sensor Data Analysis for [HackZurich](http://hackzurich.com/)

Based on [SHMACK](https://github.com/Zuehlke/SHMACK) deploy the components to perform sensor data ingestion and analysis in a [DC/OS cluster](https://mesosphere.com/product/) in the [Amazon AWS Cloud](https://aws.amazon.com/).
The data can get received from the corresponding [smartphone apps for iOS](https://github.com/Zuehlke/hackzurich-sensordata-ios) or [Android](https://github.com/Zuehlke/hackzurich-sensordata-android)

# What kind of sensor data?

So far, we are expecting and prepared to receive the different data described here: <https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md>


# Getting started
 
In order to be able to run the sensor data analysis in the cloud, you need a running cluster. 
This works best using [SHMACK](https://github.com/Zuehlke/SHMACK) with the following settings:
* Since Spark proposes In Memory computation and Cassandra/Kafka/HDFS provide their own redundancy, 
  better switch to memory-optimized EC2 `SLAVE_INSTANCE_TYPE` in `shmack_env`, e.g. [r3.xlarge](https://aws.amazon.com/ec2/pricing/).
  They are a bit more expensive, but significantly increase the data you can handle per node compared to the default (m3.xlarge).
  Spot instances are always a good choice if you want to save money and don't care if you may loose part of or your entire cluster if you get outbid.
* You may add all the packages you will need anyway in `create-stack.sh`: `INSTALL_PACKAGES="spark cassandra kafka marathon marathon-lb"`, 
  so they already get installed when you create your cluster using the CloudFormation template.
* Now you can go through the steps described in <https://github.com/Zuehlke/SHMACK> 
  from [Installation](https://github.com/Zuehlke/SHMACK#installation) to [Stack Creation](https://github.com/Zuehlke/SHMACK#stack-creation-from-now-on-you-pay-for-usage).
* Once the stack is up-and-running an you see on the DC/OS master dashboard that services are healthy,
  you can deploy the [Sensor Ingestion Akka REST Service](https://github.com/Zuehlke/hackzurich-sensordataanalysis/tree/master/sensor-ingestion#regular-deployment-into-a-shmack-dcos-cluster)
  
  

  

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