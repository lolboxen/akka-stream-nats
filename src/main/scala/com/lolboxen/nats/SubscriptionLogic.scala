package com.lolboxen.nats

import akka.stream.Attributes.Name
import akka.stream.{Attributes, FlowShape}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, StageLogging}
import com.lolboxen.nats.ConnectionSource.{Connected, Disconnected, Protocol}
import io.nats.client.Connection

abstract class SubscriptionLogic[O, S](shape: FlowShape[Protocol, O], inheritedAttributes: Attributes)
  extends GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

  private var _subscription: Option[S] = None

  setHandlers(shape.in, shape.out, this)

  override def preStart(): Unit = {
    super.preStart()
    pull(shape.in)
  }

  override def postStop(): Unit = {
    _subscription.foreach(unsubscribe)
    super.postStop()
  }

  override def onPush(): Unit = {
    grab(shape.in) match {
      case Connected(connection) =>
        _subscription = Some(subscribe(connection))
      case Disconnected =>
        _subscription.foreach(unsubscribe)
        _subscription = None
    }
    pull(shape.in)
  }

  protected def subscription: Option[S] = _subscription

  protected def subscribe(connection: Connection): S

  protected def unsubscribe(subscription: S): Unit

  protected def name: Option[String] = inheritedAttributes.get[Name].map(_.n)

  protected def logSubscriptionChange(subject: String, subscribed: Boolean): Unit = {
    val action = if (subscribed) "subscribed" else "unsubscribed"
    name match {
      case Some(n) => log.info("{} {} to {}", n, action, subject)
      case None => log.info("{} to {}", action, subject)
    }
  }
}
