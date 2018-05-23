package io.github.gatling.sql.action

import java.sql.{Connection, PreparedStatement}
import java.util.UUID
import java.util.concurrent.{Semaphore, TimeUnit}

import akka.actor.{ActorPath, ActorRef}
import com.zaxxer.hikari.HikariDataSource
import io.gatling.commons.stats.Status
import io.gatling.commons.validation.Success
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.controller.throttle.{ThrottledRequest, Throttler}
import io.gatling.core.session.el.ElCompiler
import io.gatling.core.session.{GroupBlock, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.stats.message.ResponseTimings
import io.gatling.core.stats.writer.UserMessage
import io.github.gatling.sql.PreparedSqlStatement
import io.github.gatling.sql.protocol.{SqlProtocol, SqlProtocolBuilder}
import io.github.gatling.sql.request.{Argument, SqlAttributes, SqlType}
import org.scalatest.FlatSpec

class SqlActionSpec extends FlatSpec {
  "An action" should "set arguments of prepared statements" in {
    val ds = new HikariDataSource
    val dbUrl = s"jdbc:h2:mem:${UUID.randomUUID().toString}"
    ds.setJdbcUrl(dbUrl)
    val conn = ds.getConnection
    var builder: SqlProtocolBuilder = SqlProtocolBuilder(conn)
    builder = builder.url(dbUrl)
    val protocol: SqlProtocol = builder.build()

    conn.createStatement().execute("create table t (i number)")

    val session = new Session("test", 0, Map())

    val coreComponents = CoreComponents(null, new DummyThrottler(), new DummyStatsEngine, null, null)

    val sema = new Semaphore(0)
    val prep : Connection => PreparedStatement = conn => conn.prepareStatement("insert into t(i) values (?)")
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

    val prep : Connection => PreparedStatement = conn => conn.prepareStatement("insert into t(s) values (?)")

    val session = new Session("test", 0, Map("var" -> "foo"))

    val coreComponents = CoreComponents(null, new DummyThrottler(), new DummyStatsEngine, null, null)

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

    override def logUser(userMessage: UserMessage): Unit = {}

    override def logResponse(session: Session, requestName: String, timings: ResponseTimings, status: Status, responseCode: Option[String], message: Option[String], extraInfo: List[Any]): Unit = {}

    override def logGroupEnd(session: Session, group: GroupBlock, exitTimestamp: Long): Unit = {}

    override def logCrash(session: Session, requestName: String, error: String): Unit = {}
  }

  class DummyThrottler() extends Throttler(null, null) {
    override def throttle(scenarioName: String, action: () => Unit): Unit =
      action()
  }
}
