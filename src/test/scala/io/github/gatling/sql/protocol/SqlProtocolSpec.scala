package io.github.gatling.sql.protocol

import java.util.UUID

import com.zaxxer.hikari.HikariDataSource
import org.scalatest.flatspec.AnyFlatSpec

class SqlProtocolSpec extends AnyFlatSpec {
  "A builder" should "use the supplied connection, if any" in {
    val ds = new HikariDataSource
    ds.setJdbcUrl(s"jdbc:h2:mem:${UUID.randomUUID().toString}")
    val conn = ds.getConnection
    var builder: SqlProtocolBuilder = SqlProtocolBuilder(conn)
    builder = builder.url("jdbc:h2:mem::")
    val protocol: SqlProtocol = builder.build()
    assert(protocol.connection == conn)
  }

  it should "build a new connection if none was specified" in {
    var builder: SqlProtocolBuilder = SqlProtocolBuilder()
    val url = s"jdbc:h2:mem:${UUID.randomUUID().toString}"
    builder = builder.url(url)
    val protocol: SqlProtocol = builder.build()
    assert(protocol.connection.getMetaData.getURL == url)
  }

}
