package com.lolboxen.nats

import com.lolboxen.nats.BaseSubscriptionAdapter.State
import io.nats.client.{Connection, Message}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object BaseSubscriptionAdapter {
  trait State extends SubscriptionAdapter with NatsConnectionListener {
    override def open(listener: AdapterListener): Unit = ???
    override def close(): Unit = ???
    override def fetch(batchSize: Int): Future[Seq[Message]] = ???
    override def onConnected(connection: Connection): Unit = ???
    override def onDisconnected(): Unit = ???
    override def onClosed(cause: Throwable): Unit = ???
  }
}

abstract class BaseSubscriptionAdapter(protected val natsConnection: NatsConnection,
                                       protected val subject: String)
  extends SubscriptionAdapter with NatsConnectionListener {

  protected val log: Logger = LoggerFactory.getLogger(getClass)

  protected val self: this.type = this

  private var state: State = Closed

  override def open(listener: AdapterListener): Unit = synchronized(state.open(listener))

  override def close(): Unit = synchronized(state.close())

  override def fetch(batchSize: Int): Future[Seq[Message]] = synchronized(state.fetch(batchSize))

  override def onConnected(connection: Connection): Unit = synchronized(state.onConnected(connection))

  override def onDisconnected(): Unit = synchronized(state.onDisconnected())

  override def onClosed(cause: Throwable): Unit = synchronized(state.onClosed(cause))

  protected def become(state: State): Unit = this.state = state

  protected def createSubscriptionFetch(connection: Connection): SubscriptionFetch

  protected def subscriptionInfo: String

  object Closed extends State {
    override def open(listener: AdapterListener): Unit = {
      natsConnection.connect(self)
      become(new Connecting(listener))
    }

    override def close(): Unit = ()

    override def fetch(batchSize: Int): Future[Seq[Message]] =
      throw new IllegalStateException("must open connection first")
  }

  class Connecting(listener: AdapterListener) extends State {
    override def open(listener: AdapterListener): Unit =
      throw new IllegalStateException("already called open")

    override def close(): Unit = {
      become(Closed)
      natsConnection.close()
    }

    override def fetch(batchSize: Int): Future[Seq[Message]] =
      throw new IllegalStateException("must wait for subscription")

    override def onConnected(connection: Connection): Unit = {
      log.info("nats subscription connected for {}", subscriptionInfo)
      try {
        val subscription = createSubscriptionFetch(connection)
        become(new Connected(listener, subscription))
        listener.resumeOperations()
      }
      catch {
        case cause: Throwable => listener.suspendOperationsIndefinitely(cause)
      }
    }

    override def onDisconnected(): Unit = {
      log.warn("nats subscription disconnected for {}", subscriptionInfo)
      listener.suspendOperations()
    }

    override def onClosed(cause: Throwable): Unit = listener.suspendOperationsIndefinitely(cause)
  }

  class Connected(listener: AdapterListener, subscription: SubscriptionFetch) extends State {

    override def open(listener: AdapterListener): Unit =
      throw new IllegalStateException("already called open")

    override def close(): Unit = {
      natsConnection.close()
      subscription.cancel()
      become(Closed)
    }

    override def fetch(batchSize: Int): Future[Seq[Message]] = subscription.fetch(batchSize)

    override def onConnected(connection: Connection): Unit = {
      log.info("nats subscription connected for {}", subscriptionInfo)
      listener.resumeOperations()
    }

    override def onDisconnected(): Unit = {
      log.warn("nats subscription disconnected for {}", subscriptionInfo)
      listener.suspendOperations()
    }

    override def onClosed(cause: Throwable): Unit = listener.suspendOperationsIndefinitely(cause)
  }
}
