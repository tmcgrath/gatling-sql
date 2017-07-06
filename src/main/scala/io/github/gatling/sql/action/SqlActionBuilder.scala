package io.github.gatling.sql.action

import io.github.gatling.sql.protocol.SqlProtocol
import io.github.gatling.sql.request.SqlAttributes
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.ClockSingleton.nowMillis
import io.gatling.commons.validation.Validation
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.protocol.ProtocolComponentsRegistry
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.stats.message.ResponseTimings
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import java.sql.PreparedStatement

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SqlActionBuilder(attr: SqlAttributes) extends ActionBuilder with NameGen {

  private def components(protocolComponentsRegistry: ProtocolComponentsRegistry) =
    protocolComponentsRegistry.components(SqlProtocol.SqlProtocolKey)

  override def build(ctx: ScenarioContext, next: Action): Action = {
    import ctx._
    val statsEngine = coreComponents.statsEngine
    val sqlComponents = components(protocolComponentsRegistry)
    new SqlAction(genName(s"SQL: ${attr.tag}"), sqlComponents.sqlProtocol, statsEngine, next, attr)
  }

}

class SqlAction(val name: String, protocol: SqlProtocol, val statsEngine: StatsEngine, val next: Action,
                val attr: SqlAttributes) extends ExitableAction {

  def execute(session: Session): Unit = {
    val stmt: Validation[PreparedStatement] = attr.statement(session)

    stmt.onFailure(err => {
      statsEngine.logResponse(session, name, ResponseTimings(nowMillis, nowMillis), KO, None, Some("Error setting up statement: " + err), Nil)
      next ! session.markAsFailed
    })

    stmt.onSuccess({ stmt =>
      val start = nowMillis

      val result = Future {
        stmt.execute()
      }

      result.onFailure { case t =>
        statsEngine.reportUnbuildableRequest(session, name, t.getMessage)
      }

      result.onSuccess { case result =>

        val requestEndDate = nowMillis

        statsEngine.logResponse(
          session,
          name,
          ResponseTimings(startTimestamp = start, endTimestamp = requestEndDate),
          if (result) OK else KO,
          None,
          if (result) None else Some("Failed... TBD")
        )

        next ! session.markAsSucceeded
      }
    })
  }
}
