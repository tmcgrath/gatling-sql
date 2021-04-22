package io.github.gatling.sql

import java.sql.{Connection, PreparedStatement}

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.{Failure, Success, Validation}
import io.gatling.core.session.{Expression, Session}
import io.github.gatling.sql.protocol.SqlProtocol

abstract class SqlStatement() extends StrictLogging {
  def apply(session: Session, sqlProtocol: SqlProtocol): (Session, Validation[PreparedStatement]) = null
}

class SimpleSqlStatement(statement: Expression[String]) extends SqlStatement {
  override def apply(session: Session, sqlProtocol: SqlProtocol): (Session, Validation[PreparedStatement]) = {
    statement(session) match {
      case Success(stmt) =>
      logger.debug(s"STMT: $stmt")
      (session, Success(sqlProtocol.connection.prepareStatement(stmt)))
      case Failure(t) => (session, Failure(t))
    }
  }
}

case class PreparedSqlStatement(id : String, statement: Expression[Connection => PreparedStatement]) extends SqlStatement {
  override def apply(session: Session, sqlProtocol: SqlProtocol): (Session, Validation[PreparedStatement]) = {
    statement(session) match {
      case Success(f) =>
        val key = "io.github.gatling.sql." + id
        if (session.contains(key)) {
          (session, Success(session(key).as[PreparedStatement]))
        } else {
          val stmt : PreparedStatement = f(sqlProtocol.connection)
          val newSession = session.set(key, stmt)
          (newSession, Success(stmt))
        }
      case Failure(t) => (session, Failure(t))
    }
  }
}
