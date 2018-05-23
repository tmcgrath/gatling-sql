package io.github.gatling.sql.action

import java.io.ByteArrayInputStream
import java.sql.{Connection, PreparedStatement}

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.ClockSingleton.nowMillis
import io.gatling.commons.validation.{Failure, Success, Validation}
import io.gatling.core.CoreComponents
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.protocol.ProtocolComponentsRegistry
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.stats.message.ResponseTimings
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import io.github.gatling.sql.protocol.SqlProtocol
import io.github.gatling.sql.request.{Argument, SqlAttributes, SqlType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SqlActionBuilder(attr: SqlAttributes, arguments : List[Argument] = List()) extends ActionBuilder with NameGen {

  private def components(protocolComponentsRegistry: ProtocolComponentsRegistry) =
    protocolComponentsRegistry.components(SqlProtocol.SqlProtocolKey)

  override def build(ctx: ScenarioContext, next: Action): Action = {
    import ctx._
    val sqlComponents = components(protocolComponentsRegistry)
    new SqlAction(
      genName(s"SQL: ${attr.tag}"), sqlComponents.sqlProtocol, ctx.throttled, ctx.coreComponents, next, attr, arguments,
    )
  }

}

class SqlAction(val name: String, protocol: SqlProtocol, val throttled : Boolean, val coreComponents: CoreComponents, val next: Action,
                val attr: SqlAttributes, val arguments : List[Argument]) extends ExitableAction {

  def execute(session: Session): Unit = {
    val (newSession, psExpr: Validation[PreparedStatement]) = attr.statement(session, protocol)

    psExpr match {
      case Success(stmt) =>
        if (throttled) {
          coreComponents.throttler.throttle(name, () => {
            executeStatement(stmt, newSession)
          })
        } else {
          executeStatement(stmt, newSession)
        }
      case Failure(err) =>
        statsEngine.logResponse(session, name, ResponseTimings(nowMillis, nowMillis), KO, None, Some("Error setting up statement: " + err), Nil)
        next ! newSession.markAsFailed
    }
  }

  def executeStatement(stmt: PreparedStatement, session : Session) {
      val start = nowMillis

      val future = Future {
          val resolved = arguments.map(a => {
              a.value(session) match {
                  case Success(v: Any) => (a.pos, a.sqlType, v)
                  case Failure(msg: String) =>
                      statsEngine.reportUnbuildableRequest(session, name, msg)
                      throw new IllegalArgumentException(s"Unable to evaluate expression for parameter at position ${a.pos}")
              }
          })
          resolved.foreach {
              case (pos, SqlType.INT, i: Int) => stmt.setInt(pos, i)
              case (pos, SqlType.INT, _) => throw new IllegalStateException(s"Unsupported type for INT argument")
              case (pos, SqlType.STRING, s: String) => stmt.setString(pos, s)
              case (pos, SqlType.STRING, _) => throw new IllegalStateException(s"Unsupported type for STRING argument")
              case (pos, SqlType.BLOB, a: Array[Byte]) => stmt.setBlob(pos, new ByteArrayInputStream(a))
              case (pos, SqlType.BLOB, _) => throw new IllegalStateException(s"Unsupported type for BLOB argument")
              case (_, _, _) => throw new IllegalStateException(s"Unsupported argument type")
          }
          stmt.execute()
      }

      future onComplete {
          case scala.util.Success(result) =>
              val requestEndDate = nowMillis
              statsEngine.logResponse(
                  session,
                  name,
                  ResponseTimings(startTimestamp = start, endTimestamp = requestEndDate),
                  OK,
                  None,
                  None
              )
              next ! session.markAsSucceeded
          case scala.util.Failure(t)=>
              val requestEndDate = nowMillis
              statsEngine.logResponse(
                  session,
                  name,
                  ResponseTimings(startTimestamp = start, endTimestamp = requestEndDate),
                  KO,
                  None,
                  Some(t.getMessage)
              )
              next ! session.markAsFailed
      }
  }

  override def statsEngine: StatsEngine = coreComponents.statsEngine
}
