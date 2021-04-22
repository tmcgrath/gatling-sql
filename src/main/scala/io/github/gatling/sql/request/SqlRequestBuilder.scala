package io.github.gatling.sql.request

import java.sql.{Connection, PreparedStatement}

import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.protocol.Protocols
import io.gatling.core.session.Expression
import io.github.gatling.sql.action.{CommitActionBuilder, SqlActionBuilder}
import io.github.gatling.sql.request.SqlType.SqlType
import io.github.gatling.sql.{PreparedSqlStatement, SimpleSqlStatement, SqlStatement}

case class SqlRequestBuilderBase(tag: String) {

  def selectQuery(query: Expression[String]) : SqlRequestBuilder =
    SqlRequestBuilder(SqlAttributes(tag, new SimpleSqlStatement(query)))

  def preparedQuery(id : String, query: Expression[Connection => PreparedStatement]) : SqlPreparedRequestBuilder =
    new SqlPreparedRequestBuilder(SqlAttributes(tag, new PreparedSqlStatement(id, query)))

  def update(query: Expression[String], protocols : Protocols) : SqlRequestBuilder =
    SqlRequestBuilder(SqlAttributes(tag, new SimpleSqlStatement(query)))

  def preparedUpdate(id : String, query: Expression[Connection => PreparedStatement]) : SqlPreparedRequestBuilder =
    new SqlPreparedRequestBuilder(SqlAttributes(tag, new PreparedSqlStatement(id, query)))

  def commit() : SqlCommitBuilder =
    SqlCommitBuilder()
}

case class SqlRequestBuilder(attr: SqlAttributes) {
  def build(): ActionBuilder = new SqlActionBuilder(attr)
  /**
    * See GatlingCql for example of implementing something like the following
    * Adds checks on the response
    *
    */
  //  def check(checks: CqlCheck*): SqlRequestBuilder = SqlRequestBuilder(attr.copy(checks = attr.checks ::: checks.toList))
}

case class SqlCommitBuilder() {
    def build(): ActionBuilder = new CommitActionBuilder()
}

case class SqlAttributes(tag: String, statement: SqlStatement)

object SqlType extends Enumeration {
  type SqlType = Value
  val INT, STRING, BLOB = Value
}

case class Argument(pos : Int, sqlType : SqlType, value : Expression[Any]) {
}

class SqlPreparedRequestBuilder(attr: SqlAttributes) {
  var next = 1
  var arguments : List[Argument] = List()

  def withIntArgument(pos : Int, i : Expression[Int]): SqlPreparedRequestBuilder = {
    arguments = Argument(pos, SqlType.INT, i) :: arguments
    this
  }

  def withIntArgument(i : Expression[Int]): SqlPreparedRequestBuilder = {
    withIntArgument(next, i)
    next = next + 1
    this
  }

  def withStringArgument(pos : Int, s : Expression[String]): SqlPreparedRequestBuilder = {
    arguments = Argument(pos, SqlType.STRING, s) :: arguments
    this
  }

  def withStringArgument(s : Expression[String]): SqlPreparedRequestBuilder = {
    withStringArgument(next, s)
    next = next + 1
    this
  }

  def withBlobArgument(pos : Int, bs : Expression[Array[Byte]]): SqlPreparedRequestBuilder = {
    arguments = Argument(pos, SqlType.BLOB, bs) :: arguments
    this
  }

  def withBlobArgument(bs : Expression[Array[Byte]]): SqlPreparedRequestBuilder = {
    withBlobArgument(next, bs)
    next = next + 1
    this
  }

  def build() : ActionBuilder = new SqlActionBuilder(attr, arguments)
}