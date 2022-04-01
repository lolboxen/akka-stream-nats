package com.lolboxen.nats

import akka.stream.{Attributes, FlowShape}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client.{Message, MessageHandler}

abstract class PushSubscriptionLogic[S](shape: FlowShape[Protocol, Message], inheritedAttributes: Attributes)
  extends SubscriptionLogic[Message, S](shape, inheritedAttributes) with MessageHandler {
  override def onPull(): Unit = ()

  override def onMessage(msg: Message): Unit = emit(shape.out, msg)
}
