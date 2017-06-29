package io.github.gatling.sql.db

import java.sql.{Connection, DriverManager}

import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPoolConfig}
import org.apache.commons.pool2.impl.GenericObjectPool

/**
  * Original: https://github.com/senkadam/gatlingsql
 * Created by senk on 8.1.15.
 */

/**
 * Holder of references to connection and time values
 * @param connection SQL connection to a database
 * @param time time in miliseconds
 */
case class ConnectionAndTimes(connection: Connection, time: Long)

/**
 * Mixin providing function returning proper Connection and time
 */
trait ConnectionHandling {
  /**
   * Function
   * @return SQL Connection and time
   */
  def getConnectionAndTimes: ConnectionAndTimes

  def returnConnection(connection:Connection):Unit = connection.close()
}

/**
 * Mixin providing function returning proper time including creation of SQL connection
 */
trait ConnectionTimeIncluded extends ConnectionHandling {
  /**
   * Function
   * @return SQL Connection and time starting before connection creation
   */
  override def getConnectionAndTimes = {
    val time = System.currentTimeMillis()
    val connection = ConnectionFactory.connection

    new ConnectionAndTimes(connection, time)
  }
}

/**
 * Mixin providing function returning proper time excluding creation of SQL connection
 */
trait ConnectionTimeExcluded extends ConnectionHandling {
  /**
   * Function
   * @return SQL Connection and time starting after connection creation
   */
  override def getConnectionAndTimes = {
    val connection = ConnectionFactory.connection
    val time = System.currentTimeMillis()

    new ConnectionAndTimes(connection, time)
  }
}

/**
 * Mixin providing function returning proper time excluding creation of SQL connection,
 * Connections will be reused.
 */
trait ConnectionReuse extends ConnectionHandling {

  /**
   * Function
   * @return SQL Connection and time starting after connection creation
   */
  override def getConnectionAndTimes = {
    val time = System.currentTimeMillis()

    new ConnectionAndTimes(ConnectionFactory.pool.borrowObject(), time)
  }

  override def returnConnection(connection:Connection):Unit = ConnectionFactory.pool.returnObject(connection)
}


/**
 * Factory object creating a SQL connection
 */
object ConnectionFactory {

  class ConnectionPoolFactory extends BasePooledObjectFactory[Connection]{
    override def create(): Connection = ConnectionFactory.connection

    override def wrap(connection: Connection): PooledObject[Connection] = new DefaultPooledObject[Connection](connection)

    override def destroyObject(p: PooledObject[Connection]): Unit = p.getObject.close()

    override def validateObject(p: PooledObject[Connection]): Boolean = !p.getObject.isClosed
  }
  val connectionPoolConfig = {
    val cfg = new GenericObjectPoolConfig
    cfg.setMaxTotal( System.getProperty("maxDbConnections", Integer.MAX_VALUE+"").toInt )
    cfg
  }
  class ConnectionPool extends GenericObjectPool[Connection](new ConnectionPoolFactory, connectionPoolConfig){

  }

  lazy val pool =  new ConnectionPool

  lazy val config = ConfigFactory.load

  var dBWithSet: Option[DBWithSettings]=None

  def setConnection(dBWithSettings: DBWithSettings):Unit={
    dBWithSet=Some(dBWithSettings)

  }

  def connection: Connection = dBWithSet match {
    case None => DriverManager.getConnection(
      config.getString("jdbc.db.url"),
      config.getString("jdbc.db.username"),
      config.getString("jdbc.db.password"))
    case Some(x)=>DriverManager.getConnection(
      x.url,
      x.username,
      x.password)
  }



}

case class DBWithSettings(url:String, username:String, password:String)