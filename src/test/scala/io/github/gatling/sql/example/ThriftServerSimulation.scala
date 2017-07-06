package io.github.gatling.sql.example

import io.gatling.core.Predef._
import io.github.gatling.sql.Predef._
import io.github.gatling.sql.db.ConnectionPool

class ThriftServerSimulation extends Simulation {

  Class.forName("org.apache.hive.jdbc.HiveDriver")

  val conn = ConnectionPool.connection
  val sqlConfig = sql.connection(conn)

  // Assumes data is already loaded into a Cassandra keyspace named 'pioneer'
  // and in tables `students` or `teachers`

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