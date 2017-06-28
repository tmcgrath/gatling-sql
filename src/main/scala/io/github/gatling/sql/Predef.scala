package io.github.gatling.sql

import io.gatling.core.action.builder.ActionBuilder
import io.github.gatling.sql.protocol.{SqlProtocol, SqlProtocolBuilder}
import io.github.gatling.sql.request.{SqlRequestBuilder, SqlRequestBuilderBase}

object Predef { // someday maybe - implement trait for checks; re: approach from CQL

  val sql = SqlProtocolBuilder

  implicit def sqlBuilderToProtocol(builder: SqlProtocolBuilder): SqlProtocol = builder.build()
  implicit def sqlBuilderActionBuilder(builder: SqlRequestBuilder): ActionBuilder = builder.build()

  def sql(tag: String) = SqlRequestBuilderBase(tag)
}
