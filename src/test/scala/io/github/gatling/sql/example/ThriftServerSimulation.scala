package io.github.gatling.sql.example

import io.gatling.core.Predef._
import io.github.gatling.sql.Predef._
import io.github.gatling.sql.db.ConnectionPool


class ThriftServerSimulation extends Simulation {

  Class.forName("org.apache.hive.jdbc.HiveDriver")

  val conn = ConnectionPool.connection
  val sqlConfig = sql.connection(conn)

  // Assumes data is already loaded into a Cassandra keyspace named 'killrweather'

  val joinQuery = """SELECT ws.name, raw.temperature
                  FROM killrweather.raw_weather_data raw JOIN killrweather.weather_station ws
                  ON raw.wsid=ws.id WHERE raw.wsid = '725030:14732'
                  AND raw.year = 2008 AND raw.month = 12 AND raw.day = 31"""

  val sqlAllQuery = "SELECT * FROM killrweather.weather_station WHERE ID = ${id}"

  def scn =
    scenario("test").repeat(1) {
      feed(csv("sample-feed.csv").circular).
      exec(sql("Name query").selectQuery(joinQuery)).
      exec(sql("All query").selectQuery(sqlAllQuery))
    }

  setUp(scn.inject(atOnceUsers(10)))
    .protocols(sqlConfig)

}