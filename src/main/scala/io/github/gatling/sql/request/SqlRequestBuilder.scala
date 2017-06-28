package io.github.gatling.sql.request

import io.github.gatling.sql.action.SqlActionBuilder
import io.github.gatling.sql.{SimpleSqlStatement, SqlStatement}
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression

case class SqlRequestBuilderBase(tag: String) {

  def selectQuery(query: Expression[String]) = SqlRequestBuilder(SqlAttributes(tag, SimpleSqlStatement(query)))

  /* Someday maybe
  def updateQuery(query: String) = new SqlRequestBuilder(requestName, SqlUpdateStatement(query))
  def selectQuery(requestName:String, query: String) = new SqlRequestBuilder(requestName, SqlSelectStatement(query))
  def preparedQuery(requestName:String,query: String)= new SqlRequestBuilder(requestName, SqlPreparedSelectStatement(query,dataSource))
  */
}

/* Someday maybe
case class SqlPreparedRequestParamsBuilder(tag: String, prepared: PreparedStatement) {
//  def withParams(params: Expression[AnyRef]*) = SqlRequestBuilder(CqlAttributes(tag, BoundCqlStatement(prepared, params: _*)))
//}
*/
case class SqlRequestBuilder(attr: SqlAttributes) {
  def build(): ActionBuilder = new SqlActionBuilder(attr)

  /**
    * See GatlingCql for example of implementing something like this
    * Stops defining the request and adds checks on the response
    *
    */
//  def check(checks: CqlCheck*): SqlRequestBuilder = SqlRequestBuilder(attr.copy(checks = attr.checks ::: checks.toList))
}

case class SqlAttributes(tag: String, statement: SqlStatement)
