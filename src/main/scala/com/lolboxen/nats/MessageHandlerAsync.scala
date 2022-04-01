package com.lolboxen.nats

import akka.stream.stage.GraphStageLogic
import io.nats.client.{Message, MessageHandler}

class MessageHandlerAsync(target: GraphStageLogic with MessageHandler) extends MessageHandler {
  private val onMessageCallback = target.getAsyncCallback[Message](target.onMessage)
  override def onMessage(msg: Message): Unit = onMessageCallback.invoke(msg)
}
