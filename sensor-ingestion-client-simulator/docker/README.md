Load Generator Docker Image
===========================

To build the Docker image a folder `preformatted-small` is needed, which contains the sensor data.
The `DataPreformatter` Scala class can be used to put the data in the right structure.

Use `make all` to build and publish the docker image.

Make sure that the `PROJ_DIR` variable in `build.sh` is updated.