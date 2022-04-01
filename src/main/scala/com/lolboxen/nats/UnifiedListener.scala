package com.lolboxen.nats

import io.nats.client.Connection

trait UnifiedListener {
  def onConnect(connection: Connection): Unit

  /**
   * temporary - client will attempt a reconnect
   */
  def onDisconnect(): Unit

  /**
   * permanent - reached max reconnection attempts or manually closed
   */
  def onClose(): Unit

  /**
   * some of these are handled by the client while others cause closure of client
   * onDisconnect is called in cases of closure after this function
   *
   * @param cause
   */
  def onMaybeFatalException(cause: String): Unit

  /**
   * exceptions that do not result in closure of client and were handled but are exposed here to for
   * logging or debugging purposes
   *
   * @param cause
   */
  def onNonFatalException(cause: Throwable): Unit
}
