package io.github.gatling.sql.db
import java.sql.Connection

import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.HikariDataSource

object ConnectionPool {


  def connection(): Connection = {
    lazy val config: Config = ConfigFactory.load

    val ds = new HikariDataSource
    ds.setJdbcUrl(config.getString("jdbc.db.url"))
    ds.setUsername(config.getString("jdbc.db.username"))
    ds.setPassword(config.getString("jdbc.db.password"))
    ds.getConnection
  }

}
