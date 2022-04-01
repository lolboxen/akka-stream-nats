package com.lolboxen.nats

import akka.stream.stage.GraphStageLogic
import io.nats.client.Connection

class UnifiedListenerAsync(target: GraphStageLogic with UnifiedListener) extends UnifiedListener {
  private val onConnectCallback = target.getAsyncCallback[Connection](target.onConnect)
  private val onDisconnectCallback = target.getAsyncCallback[Unit](_ => target.onDisconnect())
  private val onCloseCallback = target.getAsyncCallback[Unit](_ => target.onClose())
  private val onMaybeFatalExceptionCallback = target.getAsyncCallback[String](target.onMaybeFatalException)
  private val onNonFatalExceptionCallback = target.getAsyncCallback[Throwable](target.onNonFatalException)
  override def onConnect(connection: Connection): Unit = onConnectCallback.invoke(connection)
  override def onDisconnect(): Unit = onDisconnectCallback.invoke(())
  override def onClose(): Unit = onCloseCallback.invoke(())
  override def onMaybeFatalException(cause: String): Unit = onMaybeFatalExceptionCallback.invoke(cause)
  override def onNonFatalException(cause: Throwable): Unit = onNonFatalExceptionCallback.invoke(cause)
}
