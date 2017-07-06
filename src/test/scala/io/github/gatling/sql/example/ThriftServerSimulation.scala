package io.github.gatling.sql.example

import io.gatling.core.Predef._
import io.github.gatling.sql.Predef._
import io.github.gatling.sql.db.ConnectionPool

/**
  *
  * uncomment the hive-jdbc and hadoop-common dependecies in pom.xml to run this example
  *
  */
class ThriftServerSimulation extends Simulation {

  Class.forName("org.apache.hive.jdbc.HiveDriver")

  val conn = ConnectionPool.connection
  val sqlConfig = sql.connection(conn)

  // Assumes data is already loaded into a Cassandra keyspace named 'pioneer'
  // and tables `students` or `teachers` populated with data

  val sqlQuery = "SELECT NAME FROM pioneer.${table} WHERE ID = ${id}"
  val sqlAllQuery = "SELECT * FROM pioneer.${table} WHERE ID = ${id}"

  def scn =
    scenario("test").repeat(1) {
      feed(csv("sample-feed.csv").circular).
      exec(sql("Name query").selectQuery(sqlQuery)).
      exec(sql("All query").selectQuery(sqlAllQuery))
    }

  setUp(scn.inject(atOnceUsers(10)))
    .protocols(sqlConfig)

}