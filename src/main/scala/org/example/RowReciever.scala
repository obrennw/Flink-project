package org.example

import akka.actor.{Props, ActorSystem, Actor}
import org.apache.commons.dbcp.{PoolingDataSource, DelegatingConnection}
import org.json4s.DefaultFormats
import org.postgresql.{PGNotification, PGConnection}
import scalikejdbc._
import org.json4s.native.JsonMethods._
import scala.concurrent.duration._
import com.rabbitmq.client.{Channel, ConnectionFactory}
import com.rabbitmq.client.MessageProperties


/**
 * Simple case class to marshall to from received event.
 */
case class Product(id : Long, name: String, quantity: Long)

/**
 * Main runner. Just setups the connection pool and the actor system
 */
object RowReciever  {

  def main(args: Array[String]) {
  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/oqbrennw", "oqbrennw", "")
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)

  // initialize the actor system
  val system = ActorSystem("Hello")
  val a = system.actorOf(Props[Poller], "poller")

//  // wait for the user to stop the server
  println("Started RowReciever")
//  Console.in.read.toChar
//  system.terminate
  
  }
//  def exit() = {
//    system.terminate()
//  }
}

class Poller extends Actor {

  // execution context for the ticks
  import context.dispatcher

  val connection = ConnectionPool.borrow()
  val db: DB = DB(connection)
  val tick = context.system.scheduler.schedule(500 millis, 1000 millis, self, "tick")

  val factory = new ConnectionFactory()
  factory.setHost("localhost")
  factory.setUsername("guest")
  factory.setPassword("guest")
  factory.setPort(5672)
  factory.setVirtualHost("/")
  
  val rabbitMQConnection = factory.newConnection()
  val channel = rabbitMQConnection.createChannel()
  
  override def preStart() = {
    // make sure connection isn't closed when executing queries
    // we setup the
    db.autoClose(false)
    db.localTx { implicit session =>
      sql"LISTEN events".execute().apply()
    }
  }

  override def postStop() = {
    tick.cancel()
    db.close()
  }

  def receive = {
    case "tick" => {
      db.readOnly { implicit session =>
        val pgConnection = connection.asInstanceOf[DelegatingConnection].getInnermostDelegate.asInstanceOf[PGConnection]
        val notifications = Option(pgConnection.getNotifications).getOrElse(Array[PGNotification]())

        notifications.foreach( not => {
//            println(s"Received for: ${not.getName} from process with PID: ${not.getPID}")
//            println(s"Received data: ${not.getParameter} ")
  
            // convert to object
            implicit val formats = DefaultFormats
            val json = parse(not.getParameter) \\ "data"
            val prod = json.extract[Product]
            val message = prod.quantity.toString()
            channel.basicPublish("", "flink", MessageProperties.PERSISTENT_BASIC, message.getBytes("UTF-8"))
            println(s"Received as object: $prod\n")
          }
        )
      }
    }
  }
}