package io.github.gatling.sql

import java.sql.{Connection, PreparedStatement}
import java.util.concurrent.TimeUnit

import io.gatling.core.Predef._
import io.github.gatling.sql.Predef._
import io.github.gatling.sql.protocol.{SqlProtocol, SqlProtocolBuilder}

import scala.concurrent.duration.FiniteDuration

class ThrottledSimulation extends Simulation {

  val sqlProtocol: SqlProtocol = SqlProtocolBuilder()
    .url("jdbc:h2:mem:")
    .username("")
    .password("")
    .autocommit(true)
    .build()

  val createTableStudents =
    """CREATE TABLE students (ID int, NAME varchar(255))"""
  sqlProtocol.connection.prepareStatement(createTableStudents).execute()

  val preparedIntStatement: Connection => PreparedStatement = connection => connection.prepareStatement(
    "SELECT * FROM students WHERE id=?"
  )

  setUp(
    scenario("test").during(FiniteDuration(5, TimeUnit.SECONDS)) {
      exec(sql("Prepared ID query").preparedQuery("select", preparedIntStatement).withIntArgument(1))
    }.inject(atOnceUsers(10))
  )
    .protocols(sqlProtocol)
    .throttle(
      jumpToRps(10),
      holdFor(FiniteDuration(5, TimeUnit.SECONDS))
    ).assertions(global.successfulRequests.count.between(25, 75))
}