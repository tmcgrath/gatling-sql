package io.github.gatling.sql.example

import io.gatling.core.Predef._
import io.github.gatling.sql.Predef._
import io.github.gatling.sql.SimpleSqlStatement
import io.github.gatling.sql.db.ConnectionFactory

/**
  * Created by toddmcgrath on 6/27/17.
  */
class SimpleSimulation extends Simulation {

  val conn = ConnectionFactory.connection

  val sqlConfig = sql.connection(conn)

//  val createTableStudents="""CREATE TABLE students
//                            (ID int,
//                            NAME varchar(255))"""
//  val createTableTeachers="""CREATE TABLE teachers
//                            (ID int,
//                            NAME varchar(255))"""

//  val insertS1="""INSERT INTO students VALUES (1,'JAN')"""
//  val insertS2="""INSERT INTO students VALUES (2,'MARIE')"""
//  val insertS3="""INSERT INTO students VALUES (3,'JOSEF')"""
//
//  val insertT1="""INSERT INTO students VALUES (4,'NOVAK')"""
//  val insertT2="""INSERT INTO students VALUES (5,'CERNY')"""
//  val insertT3="""INSERT INTO students VALUES (6,'VOKATA')"""
//
//  conn.prepareStatement(createTableStudents).execute()
//  conn.prepareStatement(createTableTeachers).execute()
//  conn.prepareStatement(insertS1).execute()

//  SqlUpdateStatement(createTableTeachers).executeQuery(conn)
//
//  SqlUpdateStatement(insertS1).executeQuery(conn)
//  SqlUpdateStatement(insertS2).executeQuery(conn)
//  SqlUpdateStatement(insertS3).executeQuery(conn)
//
//  SqlUpdateStatement(insertT1).executeQuery(conn)
//  SqlUpdateStatement(insertT2).executeQuery(conn)
//  SqlUpdateStatement(insertT3).executeQuery(conn)
//

  val sqlQuery = "SELECT NAME FROM ${table} WHERE ID = ${id}"
  val sqlAllQuery = "SELECT * FROM ${table} WHERE ID = ${id}"

  def scn =
    scenario("test").repeat(1) {
      feed(csv("testDataJavaDB.csv").circular).
      exec(sql("Name query").selectQuery(sqlQuery)).
      exec(sql("All query 1").selectQuery(sqlAllQuery))
    }

  //Gatling EL for ${randomNum}"

  setUp(scn.inject(atOnceUsers(10)))
    .protocols(sqlConfig)

}

/*
  val scn = scenario("Two statements").repeat(1) { //Name your scenario
    feed(feeder)
    .exec(cql("simple SELECT")
         // 'execute' can accept a string
         // and understands Gatling expression language (EL), i.e. ${randomNum}
        .execute("SELECT * FROM test_table WHERE num = ${randomNum}")
        .check(rowCount.is(1)))
    .exec(cql("prepared INSERT")
         // alternatively 'execute' accepts a prepared statement
        .execute(prepared)
         // you need to provide parameters for that (EL is supported as well)
        .withParams(Integer.valueOf(random.nextInt()), "${randomString}")
        // and set a ConsistencyLevel optionally
        .consistencyLevel(ConsistencyLevel.ANY))
  }

  setUp(scn.inject(rampUsersPerSec(10) to 100 during (30 seconds)))
    .protocols(cqlConfig)

  after(cluster.close())
 */
