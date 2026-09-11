package com.lolboxen.nats

import akka.stream.{Attributes, FlowShape}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client.{Connection, Message, MessageHandler}

import scala.concurrent.ExecutionContext

abstract class PushSubscriptionLogic[S](executionContext: ExecutionContext,
                                        shape: FlowShape[Protocol, Message],
                                        inheritedAttributes: Attributes)
  extends SubscriptionLogic[Message, S](executionContext, shape, inheritedAttributes) with MessageHandler {

  // noop since messages are pushed from the server
  override def onPull(): Unit = ()

  override protected def onConnected(connection: Connection): Unit = ()
  override protected def onDisconnected(): Unit                    = ()
  override protected def onSubscription(subscription: S): Unit     = ()

  override def onMessage(msg: Message): Unit = emit(shape.out, msg)
}
