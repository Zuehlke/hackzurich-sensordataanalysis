#!/bin/sh

# Make sure to correctly set this path!
PROJ_DIR="/home/bewe/shmack/hackzurich-sensordataanalysis"
CUR_DIR="$(pwd)"

cd $PROJ_DIR
./gradlew :sensor-ingestion-client-simulator:fatJar
echo $CUR_DIR
cd $CUR_DIR
cp $PROJ_DIR/sensor-ingestion-client-simulator/build/libs/sensor-ingestion-client-simulator-all.jar .
docker build -t bwedenik/smack-load-generator-small .


