FROM java
MAINTAINER "Benedikt.Wedenik@zuehlke.com"
RUN ["mkdir", "/tmp/sensor-data/"]
COPY preformatted_small/ /tmp/sensor-data/
COPY sensor-ingestion-client-simulator-all.jar /tmp/app.jar
ENTRYPOINT ["java", "-jar", "/tmp/app.jar", "--data-basis=/tmp/sensor-data", "--auth=hackzurich:hackzurich"]
