package com.lolboxen.nats

import io.nats.client.api.PublishAck
import io.nats.client.{Connection, JetStreamOptions, Message}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

object Publisher {
  type Factory = Connection => Publisher

  def core(connection: Connection): Publisher =
    message => {
      connection.publish(message)
      Future.successful(None)
    }

  def jetStream(jso: JetStreamOptions)(connection: Connection): Publisher = {
    val stream = connection.jetStream(jso)
    message =>
      Option(stream.publishAsync(message))
        .map(_.asScala.map(Option.apply)(ExecutionContext.parasitic))
        .getOrElse(Future.successful(None))
  }
}

trait Publisher {
  def apply(message: Message): Future[Option[PublishAck]]
}