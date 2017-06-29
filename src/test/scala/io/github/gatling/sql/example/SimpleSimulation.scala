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

  // setup
  val dropTableStudents = """DROP TABLE students"""
  val dropTableTeachers = """DROP TABLE teachers"""

  val createTableStudents="""CREATE TABLE students
                            (ID int,
                            NAME varchar(255))"""
  val createTableTeachers="""CREATE TABLE teachers
                            (ID int,
                            NAME varchar(255))"""

  val insertS1="""INSERT INTO students VALUES (1,'JAN')"""
  val insertS2="""INSERT INTO students VALUES (2,'MARIE')"""
  val insertS3="""INSERT INTO students VALUES (3,'JOSEF')"""

  val insertT1="""INSERT INTO students VALUES (4,'NOVAK')"""
  val insertT2="""INSERT INTO students VALUES (5,'CERNY')"""
  val insertT3="""INSERT INTO students VALUES (6,'VOKATA')"""

  val stmts = Array[String](dropTableStudents, dropTableTeachers, createTableStudents,
                            createTableTeachers, insertS1, insertS2, insertS3,
                            insertT1, insertT2, insertT3)
  stmts.foreach { stmt =>
    conn.prepareStatement(stmt).execute()
  }
  // end setup

  val sqlQuery = "SELECT NAME FROM ${table} WHERE ID = ${id}"
  val sqlAllQuery = "SELECT * FROM ${table} WHERE ID = ${id}"

  def scn =
    scenario("test").repeat(1) {
      feed(csv("testDataJavaDB.csv").circular).
      exec(sql("Name query").selectQuery(sqlQuery)).
      exec(sql("All query 1").selectQuery(sqlAllQuery))
    }

  setUp(scn.inject(atOnceUsers(10)))
    .protocols(sqlConfig)

}