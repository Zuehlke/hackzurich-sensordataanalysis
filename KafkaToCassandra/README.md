1. Install cassandra
2. Install zeppeling with dcos package install --package-version=0.6.0 zeppelin
2.1 Add the labels HAPROXY_GROUP=external and HAPROXY_0_PORT=8085
2.2 Change the cassandra interpreter settings hosts properies to: node-0.cassandra.mesos
2.3 Read the connection with "dcos cassandra connection"