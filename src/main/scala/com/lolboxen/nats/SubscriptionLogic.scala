package com.lolboxen.nats

import akka.stream.Attributes.Name
import akka.stream.{Attributes, FlowShape, StreamDetachedException}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, StageLogging}
import com.lolboxen.nats.ConnectionSource.{Connected, Disconnected, Protocol}
import io.nats.client.Connection

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

abstract class SubscriptionLogic[O, S](executionContext: ExecutionContext,
                                       shape: FlowShape[Protocol, O],
                                       inheritedAttributes: Attributes)
  extends GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

  private var _subscription: Option[S] = None
  private var subscriptionInFlight: Boolean = false

  private val subscribeCallback = getAsyncCallback[Try[S]](processSubscription)

  setHandlers(shape.in, shape.out, this)

  override def preStart(): Unit = {
    super.preStart()
    pull(shape.in)
  }

  override def postStop(): Unit = {
    _subscription.foreach(unsubscribe)
    _subscription = None
    logSubscriptionChange(false)
    super.postStop()
  }

  override def onPush(): Unit = {
    grab(shape.in) match {
      case Connected(connection) =>
        subscribeIfNeeded(connection)
        onConnected(connection)
      case Disconnected => onDisconnected()
    }
    pull(shape.in)
  }

  protected def onConnected(connection: Connection): Unit

  protected def onDisconnected(): Unit

  protected def subscription: Option[S] = _subscription

  // Runs on executionContext; implementations must not access stage state.
  protected def subscribe(connection: Connection): S

  protected def unsubscribe(subscription: S): Unit

  protected def onSubscription(subscription: S): Unit

  protected def name: Option[String] = None

  private def loggingName: Option[String] = attributeName.orElse(name)

  private def attributeName: Option[String] = inheritedAttributes.get[Name].map(_.n)

  private def subscribeIfNeeded(connection: Connection): Unit =
    if (_subscription.isEmpty && !subscriptionInFlight) {
      subscriptionInFlight = true
      Future(subscribe(connection))(executionContext)
        .transformWith { x =>
          subscribeCallback.invokeWithFeedback(x).failed.map(x -> _)(ExecutionContext.parasitic)
        }(ExecutionContext.parasitic)
        .onComplete {
          case Success(Success(sub) -> (x: StreamDetachedException)) => unsubscribe(sub)
          case _ =>
        }(ExecutionContext.parasitic)
    }

  private def processSubscription(result: Try[S]): Unit = {
    subscriptionInFlight = false
    result match {
      case Success(value) if isClosed(shape.out) => unsubscribe(value)
      case Success(value) =>
        logSubscriptionChange(true)
        _subscription = Some(value)
        onSubscription(value)
      case Failure(cause) => failStage(cause)
    }
  }

  private def logSubscriptionChange(subscribed: Boolean): Unit = {
    val action = if (subscribed) "subscribed" else "unsubscribed"
    val to = name.map(x => s" to $x").getOrElse("")
    log.info("{}{}", action, to)
  }
}
