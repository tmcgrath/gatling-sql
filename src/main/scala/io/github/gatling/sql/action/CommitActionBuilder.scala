package io.github.gatling.sql.action

import io.gatling.commons.stats.OK
import io.gatling.commons.util.Clock
import io.gatling.core.CoreComponents
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.protocol.ProtocolComponentsRegistry
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import io.github.gatling.sql.protocol.SqlProtocol

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CommitActionBuilder() extends ActionBuilder with NameGen {

  private def components(protocolComponentsRegistry: ProtocolComponentsRegistry) =
    protocolComponentsRegistry.components(SqlProtocol.SqlProtocolKey)

  override def build(ctx: ScenarioContext, next: Action): Action = {
    import ctx._
    val statsEngine = coreComponents.statsEngine
    val sqlComponents = components(protocolComponentsRegistry)
    new CommitAction(genName(s"SQL: commit"), sqlComponents.sqlProtocol, statsEngine, coreComponents, next)
  }

}

class CommitAction(val name: String, protocol: SqlProtocol, val statsEngine: StatsEngine, val coreComponents : CoreComponents, val next: Action) extends ExitableAction {

  def execute(session: Session): Unit = {

      val start = System.currentTimeMillis()

      val future = Future {
        protocol.connection.commit()
      }

      future onComplete {
        case scala.util.Success(result) => val requestEndDate = System.currentTimeMillis()

          statsEngine.logResponse(
            session.scenario, session.groups,
            name,
            start, requestEndDate,
            OK,
            None,
            None
          )

          next ! session.markAsSucceeded

        case scala.util.Failure(t)=>statsEngine.reportUnbuildableRequest(
          session.scenario, session.groups, name, t.getMessage
        )

      }
  }

  override def clock: Clock = coreComponents.clock
}
