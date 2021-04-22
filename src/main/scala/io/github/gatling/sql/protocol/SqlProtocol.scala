package io.github.gatling.sql.protocol

import java.sql.Connection

import akka.actor.ActorSystem
import com.zaxxer.hikari.HikariDataSource
import io.gatling.core
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolComponents, ProtocolKey}
import io.gatling.core.session.Session


case class SqlProtocol(connection: Connection) extends Protocol {
  type Components = SqlComponents
}

object SqlProtocol {

  val SqlProtocolKey = new ProtocolKey {

    type Protocol = SqlProtocol
    type Components = SqlComponents

    override def protocolClass: Class[core.protocol.Protocol] = classOf[SqlProtocol].asInstanceOf[Class[io.gatling.core.protocol.Protocol]]

    override def defaultProtocolValue(configuration: GatlingConfiguration): SqlProtocol = throw new IllegalStateException("Can't provide a default value for SqlProtocol")

    override def newComponents(system: ActorSystem, coreComponents: CoreComponents): SqlProtocol => SqlComponents = {
      sqlProtocol => SqlComponents(sqlProtocol)
    }
  }
}

case class SqlComponents(sqlProtocol: SqlProtocol) extends ProtocolComponents {
  def onStart: Option[Session => Session] = None

  def onExit: Option[Session => Unit] = None
}

case class SqlProtocolBuilder(connection: Connection = null) {
  private var url: String = _
  private var username: String = _
  private var password: String = _
  private var autocommit: Boolean = true

  def url(url: String): SqlProtocolBuilder = {
    this.url = url
    this
  }

  def username(username: String): SqlProtocolBuilder = {
    this.username = username
    this
  }

  def password(password: String): SqlProtocolBuilder = {
    this.password = password
    this
  }

  def autocommit(autocommit: Boolean): SqlProtocolBuilder = {
    this.autocommit = autocommit
    this
  }

  def build(): SqlProtocol = {
    var conn: Connection = null
    if (connection != null) {
      conn = connection
    } else {
      val ds = new HikariDataSource
      ds.setJdbcUrl(url)
      ds.setUsername(username)
      ds.setPassword(password)
      ds.setAutoCommit(autocommit)
      conn = ds.getConnection
    }
    SqlProtocol(conn)
  }
}

object SqlProtocolBuilder {
  def connection(connection: Connection) = SqlProtocolBuilder(connection)
}