package com.lolboxen.nats

import io.nats.client.ConnectionListener.Events
import io.nats.client.{Connection, ConnectionListener, ErrorListener}

class UnifiedListenerAdapter(unifiedListener: UnifiedListener) extends ConnectionListener with ErrorListener {
  private var _madeInitialConnection: Boolean = false

  override def connectionEvent(conn: Connection, `type`: ConnectionListener.Events): Unit = {
    `type` match {
      case Events.CONNECTED =>
        _madeInitialConnection = true
        unifiedListener.onConnect(conn)
      case Events.RECONNECTED => unifiedListener.onConnect(conn)
      case Events.DISCONNECTED => unifiedListener.onDisconnect()
      case Events.CLOSED if _madeInitialConnection => unifiedListener.onClose()
      case Events.CLOSED => // block as we have not achieved initial connection
      case _ =>
    }
  }

  override def errorOccurred(conn: Connection, error: String): Unit = unifiedListener.onMaybeFatalException(error)

  override def exceptionOccurred(conn: Connection, exp: Exception): Unit = unifiedListener.onNonFatalException(exp)
}
