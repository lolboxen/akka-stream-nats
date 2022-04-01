package com.lolboxen.nats

import akka.Done
import akka.actor.Cancellable
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.lolboxen.nats.ConnectionSource.{Connected, ControlAsync, Disconnected, Protocol}
import io.nats.client._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object ConnectionSource {
  sealed trait Protocol
  case class Connected(connection: Connection) extends Protocol
  case object Disconnected extends Protocol

  class ControlAsync(target: GraphStageLogic with Control) extends Control {
    private val callback = target.getAsyncCallback[Unit](_ => target.shutdown())
    override def shutdown(): Unit = callback.invoke(())
    override def whenShutdown: Future[Done] = target.whenShutdown
  }
}

class ConnectionSource(connector: Connector)
  extends GraphStageWithMaterializedValue[SourceShape[Protocol], Control] {

  protected val out: Outlet[Protocol] = Outlet("ConnectionSource.out")

  override def shape: SourceShape[Protocol] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val stageLogic: GraphStageLogic with Control = new GraphStageLogic(shape)
      with Control with OutHandler with UnifiedListener with StageLogging {

      val shutdownPromise: Promise[Done] = Promise[Done]()
      var initialConnectionCancellable: Option[Cancellable] = None
      var connection: Option[Connection] = None

      setHandler(out, this)

      override def preStart(): Unit = {
        super.preStart()
        initialConnectionCancellable = Some(connector(new UnifiedListenerAsync(this)))
      }

      override def postStop(): Unit = {
        Try(initialConnectionCancellable.foreach(_.cancel()))
        connection match {
          case None => shutdownPromise.trySuccess(Done)
          case Some(connection) =>
            Try(connection.close()) match {
              case Success(_) => shutdownPromise.trySuccess(Done)
              case Failure(cause) => shutdownPromise.tryFailure(cause)
            }
        }
        super.postStop()
      }

      override def shutdown(): Unit = {
        Try(initialConnectionCancellable.foreach(_.cancel()))
        initialConnectionCancellable = None
        connection.foreach { connection =>
          Try(connection.close()) match {
            case Success(_) =>
            case Failure(cause) =>
              shutdownPromise.tryFailure(cause)
              failStage(cause)
          }
        }
        connection = None
      }

      override def whenShutdown: Future[Done] = shutdownPromise.future

      override def onPull(): Unit = {}

      override def onConnect(connection: Connection): Unit = {
        this.connection = Some(connection)
        initialConnectionCancellable = None
        if (!isClosed(out)) emit(out, Connected(connection))
      }

      override def onDisconnect(): Unit = {
        val hadConnection = connection.nonEmpty
        connection = None
        if (!isClosed(out) && hadConnection) emit(out, Disconnected)
      }

      override def onClose(): Unit = {
        connection = None
        completeStage()
      }

      override def onMaybeFatalException(cause: String): Unit =
        log.error("received possible fatal error from nats connection: {}", cause)

      override def onNonFatalException(cause: Throwable): Unit =
        log.error(cause, "received non fatal error from nats connection")
    }

    stageLogic -> new ControlAsync(stageLogic)
  }
}
