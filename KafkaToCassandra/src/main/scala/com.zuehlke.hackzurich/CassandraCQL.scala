package com.zuehlke.hackzurich

import com.datastax.driver.core.Session

object CassandraCQL {

  def createSchema(s: Session): Unit = {
    s.execute("CREATE KEYSPACE IF NOT EXISTS sensordata WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 }")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.batteryhistory( deviceid text, date text, batterystate text, batterylevel double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.batterycurrent( deviceid text, date text, batterystate text, batterylevel double, PRIMARY KEY (deviceid) )")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.sensorreading_by_device_sensortype( deviceid text, sensortype text, date text, x double, y double, z double, PRIMARY KEY (deviceid, sensortype, date) ) WITH CLUSTERING ORDER BY (sensortype ASC, date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.gyro( date text, deviceid text, x double, y double, z double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.accelerometer( date text, deviceid text, x double, y double, z double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.magnetometer( date text, deviceid text, x double, y double, z double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.barometer( date text, deviceid text, relativealtitude double, pressure double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.motion( date text, deviceid text, x double, w double, y double, z double, m13 double, m12 double, m33 double, m32 double, m31 double, m21 double, m11 double, m22 double, m23 double, pitch double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.microphone( date text, deviceid text, peakPower double, averagePower double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
    s.execute("CREATE TABLE IF NOT EXISTS sensordata.light( date text, deviceid text, brightnes double, PRIMARY KEY (deviceid, date) ) WITH CLUSTERING ORDER BY (date DESC)")
  }

}
