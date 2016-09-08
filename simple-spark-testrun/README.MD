Just a very basic subproject that contains a Spark job similar to [SHMACK 'Check Installation' Zeppelin Tutorial](https://www.zeppelinhub.com/viewer/notebooks/aHR0cHM6Ly9yYXcuZ2l0aHVidXNlcmNvbnRlbnQuY29tL1p1ZWhsa2UvU0hNQUNLL21hc3Rlci8wNF9pbXBsZW1lbnRhdGlvbi96ZXBwZWxpbi10dXRvcmlhbHMvQ2hlY2slMjBJbnN0YWxsYXRpb24uanNvbg).
Main intention is to verify that job can get executed in DC/OS and provide a simple baseline for the implementation of other subproejcts to implement particular jobs.

1. Start the Mesos cluster
2. Install Spark `dcos package install spark` and if you did that before, 
   but not from shell then `dcos package install --cli spark` to get the `dcos spark` commandline tool.
3. Create a fat jar containing all dependencies defined in Gradle with `./gradlew fatJarForSparkSubmit` 
   ... ready to be [submitted to Spark](http://spark.apache.org/docs/latest/submitting-applications.html)
5. Upload the fat jar to a place accessible from the cluster e.g. S3.
   Hint: Commandline `aws s3 cp <local file> <s3://destination file>` and `aws s3 sync <local dir> <s3://dir>` 
   works significantly faster than upload [in browser in S3 console](https://console.aws.amazon.com/s3/home?region=us-west-1),
   e.g. `aws s3 cp build/libs/simple-spark-testrun-all.jar s3://myjardirectory`
   but you will *always after an upload* have to adjust permissions in the S3 properties of the JAR readable/downloadable by `Everyone` in the browser.
   And when you do this the first time, just copy also the https-link of your JAR on S3 from the properties to your clipboard 
   as you will need it in the next step.
6. [Run the spark job](https://docs.mesosphere.com/1.7/usage/service-guides/spark/run-job/) with:  `dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.SparkTestrun jar-location"`
7. You can trace execution in `open-shmack-spark-ui.sh`. 
   Information is not ideally exposed since some links to the Spark UI may not work through the proxy, 
   but if needed, you can access the logs of the sandbox in `open-shmack-mesos-console.sh`; even as a browser window that updates with the logs 
   when navigating through frameworks -> spark -> task -> stdout / stderr.
