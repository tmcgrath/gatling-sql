package io.github.gatling.sql

import java.sql.{Connection, PreparedStatement}

import com.typesafe.scalalogging.StrictLogging
import io.github.gatling.sql.db.ConnectionPool
import io.gatling.commons.validation.Validation
import io.gatling.core.session.{Expression, Session}
import io.gatling.commons.validation._

trait SqlStatement extends StrictLogging {

  def apply(session:Session): Validation[PreparedStatement]

  def connection = ConnectionPool.connection
}

case class SimpleSqlStatement(statement: Expression[String]) extends SqlStatement {
  def apply(session: Session): Validation[PreparedStatement] = statement(session).flatMap { stmt =>
      logger.debug(s"STMT: ${stmt}")
      connection.prepareStatement(stmt).success
    }
}
