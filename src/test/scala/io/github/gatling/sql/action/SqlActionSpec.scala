package io.github.gatling.sql.action

import java.sql.{Connection, PreparedStatement}
import java.util.UUID
import java.util.concurrent.{Semaphore, TimeUnit}

import akka.actor.ActorRef
import com.zaxxer.hikari.HikariDataSource
import io.gatling.commons.stats.Status
import io.gatling.commons.validation.Success
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.Session.NothingOnExit
import io.gatling.core.session.el.ElCompiler
import io.gatling.core.session.{GroupBlock, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.stats.writer.UserEndMessage
import io.github.gatling.sql.PreparedSqlStatement
import io.github.gatling.sql.protocol.{SqlProtocol, SqlProtocolBuilder}
import io.github.gatling.sql.request.{Argument, SqlAttributes, SqlType}
import io.netty.channel.{DefaultEventLoop, EventLoop}
import org.scalatest.flatspec.AnyFlatSpec

class SqlActionSpec extends AnyFlatSpec {
  "An action" should "set arguments of prepared statements" in {
    val ds = new HikariDataSource
    val dbUrl = s"jdbc:h2:mem:${UUID.randomUUID().toString}"
    ds.setJdbcUrl(dbUrl)
    val conn = ds.getConnection
    var builder: SqlProtocolBuilder = SqlProtocolBuilder(conn)
    builder = builder.url(dbUrl)
    val protocol: SqlProtocol = builder.build()

    conn.createStatement().execute("create table t (i number)")
    val eventloop : EventLoop = new DefaultEventLoop()

    val session = new Session("test", 0, Map(), io.gatling.commons.stats.OK, List(), NothingOnExit, eventloop)

    val coreComponents = new CoreComponents(
      null, null, null, Option(new DummyThrottler()), new DummyStatsEngine, null, null, null
    )

    val sema = new Semaphore(0)
    val prep: Connection => PreparedStatement = conn => conn.prepareStatement("insert into t(i) values (?)")
    val insert: SqlAction = new SqlAction(
      "test", protocol, false, coreComponents, semaReleaseAction(sema),
      SqlAttributes("tag", new PreparedSqlStatement("a", session => Success(prep))),
      List(Argument(1, SqlType.INT, session => Success(42)))
    )
    insert.execute(session)

    assert(sema.tryAcquire(10, TimeUnit.SECONDS))

    val rs = conn.createStatement().executeQuery("select i from t")
    assert(rs.next())
    System.out.println(rs.getString(1))
    assert(rs.getInt(1) == 42)
  }

  it should "set arguments of prepared statements using EL" in {
    val ds = new HikariDataSource
    val dbUrl = s"jdbc:h2:mem:${UUID.randomUUID().toString}"
    ds.setJdbcUrl(dbUrl)
    val conn = ds.getConnection
    var builder: SqlProtocolBuilder = SqlProtocolBuilder(conn)
    builder = builder.url(dbUrl)
    val protocol: SqlProtocol = builder.build()

    conn.createStatement().execute("create table t (s varchar(32))")

    val prep: Connection => PreparedStatement = conn => conn.prepareStatement("insert into t(s) values (?)")
    val eventloop : EventLoop = new DefaultEventLoop()

    val session = new Session("test", 0, Map("var" -> "foo"), io.gatling.commons.stats.OK, List(), NothingOnExit, eventloop)

    val coreComponents = new CoreComponents(null, null, null, Option(new DummyThrottler()), new DummyStatsEngine, null, null, null)

    val sema = new Semaphore(0)
    val insert: SqlAction = new SqlAction(
      "test", protocol, false, coreComponents, semaReleaseAction(sema),
      SqlAttributes("tag", new PreparedSqlStatement("a", session => Success(prep))),
      List(Argument(1, SqlType.STRING, ElCompiler.compile[String]("${var}")))
    )
    insert.execute(session)

    assert(sema.tryAcquire(10, TimeUnit.SECONDS))

    val rs = conn.createStatement().executeQuery("select s from t")
    assert(rs.next())
    System.out.println(rs.getString(1))
    assert(rs.getString(1) == "foo")
  }

  private def semaReleaseAction(sema: Semaphore) = {
    new Action {
      override def name: String = "done"

      override def execute(session: Session): Unit = {
        sema.release()
      }
    }
  }

  class DummyStatsEngine extends StatsEngine() {
    override def start(): Unit = {}

    override def stop(replyTo: ActorRef, exception: Option[Exception]): Unit = {}

    override def logUserStart(scenario: String, timestamp: Long): Unit = {}

    override def logUserEnd(userMessage: UserEndMessage): Unit = {}

    override def logResponse(scenario: String, groups: List[String], requestName: String, startTimestamp: Long, endTimestamp: Long, status: Status, responseCode: Option[String], message: Option[String]): Unit = {}

    override def logGroupEnd(scenario: String, groupBlock: GroupBlock, exitTimestamp: Long): Unit = {}

    override def logCrash(scenario: String, groups: List[String], requestName: String, error: String): Unit = {}
  }

  class DummyThrottler() extends Throttler(null, null) {
    override def throttle(scenarioName: String, action: () => Unit): Unit =
      action()
  }

}
