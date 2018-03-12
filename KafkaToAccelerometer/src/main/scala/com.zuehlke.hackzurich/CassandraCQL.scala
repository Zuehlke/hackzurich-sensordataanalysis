package com.zuehlke.hackzurich

import com.datastax.driver.core.Session

object CassandraCQL {

  def createSchema(s: Session): Unit = {
    s.execute("CREATE KEYSPACE IF NOT EXISTS sensordata WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 }")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.accelerometer( date timestamp, deviceid text, x double, y double, z double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
  }

}
