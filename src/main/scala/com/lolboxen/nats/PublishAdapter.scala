package com.lolboxen.nats

import io.nats.client.Connection.Status
import io.nats.client.api.PublishAck
import io.nats.client.{Connection, JetStream, Message}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait PublishAdapter {
  def open(listener: AdapterListener): Unit
  def close(): Unit
  def apply(message: Message)(implicit ec: ExecutionContext): Future[Option[PublishAck]]
}

class JetStreamPublishAdapter(natsConnection: NatsConnection)
  extends PublishAdapter
    with NatsConnectionListener {

  private val log = LoggerFactory.getLogger(getClass)

  private var listener: AdapterListener = _
  private var jetStream: JetStream = _
  private var connection: Connection = _

  override def open(listener: AdapterListener): Unit = {
    this.listener = listener
    natsConnection.connect(this)
  }

  override def close(): Unit = if (connection != null) connection.close()

  override def apply(message: Message)(implicit ec: ExecutionContext): Future[Option[PublishAck]] = {
    import scala.jdk.FutureConverters._
    val promise = Promise[Option[PublishAck]]()
    if (canPublish) {
      jetStream.publishAsync(message).asScala.onComplete {
        case Success(ack) => promise.success(Some(ack))
        case Failure(exception) => promise.failure(exception)
      }
    }
    else promise.success(None)
    promise.future
  }

  override def onConnected(connection: Connection): Unit = {
    log.info("nats publisher connected")
    this.connection = connection
    jetStream = connection.jetStream()
    listener.resumeOperations()
  }

  override def onDisconnected(): Unit = {
    log.warn("nats publisher disconnected")
    listener.suspendOperations()
  }

  override def onClosed(cause: Throwable): Unit = listener.suspendOperationsIndefinitely(cause)

  private def canPublish: Boolean = {
    val status = connection.getStatus
    status != Status.RECONNECTING && status != Status.DISCONNECTED
  }
}
