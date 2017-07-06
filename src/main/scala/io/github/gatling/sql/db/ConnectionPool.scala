package io.github.gatling.sql.db
import java.sql.Connection

import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.HikariDataSource

object ConnectionPool {

  lazy val config: Config = ConfigFactory.load

  private val ds = new HikariDataSource
  ds.setJdbcUrl(config.getString("jdbc.db.url"))
  ds.setUsername(config.getString("jdbc.db.username"))
  ds.setPassword(config.getString("jdbc.db.password"))

  val connection: Connection = ds.getConnection

}
