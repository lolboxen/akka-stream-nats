package com.lolboxen.nats

import io.nats.client
import io.nats.client.ConnectionListener.Events
import io.nats.client._
import org.slf4j.LoggerFactory

trait NatsConnectionListener {
  def onConnected(connection: Connection): Unit
  def onDisconnected(): Unit
  def onClosed(cause: Throwable): Unit
}

class NatsConnection(url: String)
  extends ConnectionListener
    with ErrorListener {

  private val log = LoggerFactory.getLogger(classOf[NatsConnection])
  private var maybeConnection: Option[Connection] = None
  private var listener: NatsConnectionListener = _

  def connect(listener: NatsConnectionListener): Unit = {
    this.listener = listener
    if (maybeConnection.isEmpty) {
      val conOps = new client.Options.Builder()
        .server(url)
        .maxReconnects(-1)
        .maxMessagesInOutgoingQueue(0)
        .connectionListener(this)
        .errorListener(this)
        .build()
      Nats.connectAsynchronously(conOps, true)
    }
  }

  def close(): Unit = maybeConnection.foreach(_.close())

  override def connectionEvent(conn: Connection, `type`: ConnectionListener.Events): Unit = {
    maybeConnection = Some(conn)
    `type` match {
      case Events.CONNECTED => listener.onConnected(conn)
      case Events.RECONNECTED => listener.onConnected(conn)
      case Events.DISCONNECTED => listener.onDisconnected()
      case _ =>
    }
  }

  override def errorOccurred(conn: Connection, error: String): Unit = {
    conn.close()
    listener.onClosed(new Exception(error))
  }

  override def exceptionOccurred(conn: Connection, exp: Exception): Unit = log.error("nats exception occurred", exp)
}
