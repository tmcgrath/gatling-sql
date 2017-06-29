package io.github.gatling.sql

import java.sql.{Connection, PreparedStatement}

import com.typesafe.scalalogging.StrictLogging
import io.github.gatling.sql.db.{ConnectionAndTimes, ConnectionFactory}
import io.gatling.commons.validation.Validation
import io.gatling.core.session.{Expression, Session}
import io.gatling.commons.validation._

trait SqlStatement extends StrictLogging {

  def apply(session:Session): Validation[PreparedStatement]

  def getConnectionAndTimes = {
    val time = System.currentTimeMillis()
    new ConnectionAndTimes(ConnectionFactory.pool.borrowObject(), time)
  }

  def returnConnection(connection:Connection):Unit = ConnectionFactory.pool.returnObject(connection)
}

case class SimpleSqlStatement(statement: Expression[String]) extends SqlStatement {
  def apply(session: Session): Validation[PreparedStatement] = statement(session).flatMap { stmt =>
      logger.info(s"STMT: ${stmt}")
      getConnectionAndTimes.connection.prepareStatement(stmt).success
    }
}

/*
 Someday maybe support something like the following from Cql extension but for JDBC

case class SimpleCqlStatementWithParams(statement: Expression[String], parameters: Expression[Seq[AnyRef]]) extends SqlStatement {
  def apply(session:Session): Validation[PreparedStatement] = {
    statement(session).flatMap(
      stmt => {
        parameters(session).flatMap(
          params => new SimpleStatement(stmt, params.map(p => p): _*).success
        )
      }
    )
  }
}

case class BoundCqlStatement(statement: PreparedStatement, params: Expression[AnyRef]*) extends SqlStatement {
  def apply(session:Session): Validation[PreparedStatement] = {
    val parsedParams = params.map(param => param(session))
    val (validParsedParams, failures) = parsedParams.partition {case Success(s) => true; case _ => false}
    failures.toList match {
      case x :: xs => x match {
        case Failure(error) => error.failure
      }
      case _ => try {
        statement.bind(validParsedParams.map(_.get): _*).success
      } catch {
        case e: Exception => e.getMessage().failure
      }
    }
  }
}
*/
