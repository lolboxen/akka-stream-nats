package com.lolboxen.nats

import akka.stream.Attributes.Name
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import akka.stream.{Attributes, Inlet, Outlet, Shape}
import com.lolboxen.nats.ConnectionSource.{Connected, Disconnected, Protocol}
import com.lolboxen.nats.PublishFlow.{MessageTracker, PublishShape}
import com.lolboxen.nats.Publisher.Factory
import io.nats.client.api.PublishAck
import io.nats.client.{Connection, Message}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object PublishFlow {

  case class MessageTracker[C](promise: Promise[(Option[PublishAck], Message, C)], message: Message, context: C)

  final case class PublishShape[C](message: Inlet[(Message, C)],
                                        out: Outlet[Future[(Option[PublishAck], Message, C)]],
                                        protocol: Inlet[Protocol])
    extends Shape {

    override val inlets: immutable.Seq[Inlet[_]] = message :: protocol :: Nil
    override val outlets: immutable.Seq[Outlet[_]] = out :: Nil

    override def deepCopy(): PublishShape[C] =
      PublishShape(message.carbonCopy(), out.carbonCopy(), protocol.carbonCopy())
  }
}

class PublishFlow[C](publisherFactory: Factory) extends GraphStage[PublishShape[C]] {

  val messageIn: Inlet[(Message, C)] = Inlet("PublishFlow.Message.in")
  val protocolIn: Inlet[Protocol] = Inlet("PublishFlow.Protocol.in")
  val out: Outlet[Future[(Option[PublishAck], Message, C)]] = Outlet("PublishFlow.out")

  override val shape: PublishShape[C] = PublishShape(messageIn, out, protocolIn)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PublishFlowLogic(publisherFactory, inheritedAttributes, shape)
}

class PublishFlowLogic[C](publisherFactory: Factory,
                          inheritedAttributes: Attributes,
                          shape: PublishShape[C]) extends GraphStageLogic(shape) with StageLogging {
  private val publishCompletionCallback = getAsyncCallback(publishCompletion)
  private val pendingAck = ListBuffer.empty[MessageTracker[C]]
  private var stream = Option.empty[Publisher]
  // messages that were delivered but not acked can fail as much as two minutes later across a reconnect event
  // connection version will filter out those publish promises
  private var connectionVersion = 0

  setHandler(shape.message, Source)
  setHandler(shape.protocol, ConnectionHandler)
  setHandler(shape.out, NullDemand)

  override def preStart(): Unit = {
    super.preStart()
    pull(shape.protocol)
  }

  private def resumeOperations(connection: Connection): Unit = {
    logSubscriptionChange(true)
    connectionVersion += 1
    setHandler(shape.out, Demand)
    stream = Some(publisherFactory(connection))
    pendingAck.foreach(publish)
    if (isAvailable(shape.out) && !hasBeenPulled(shape.message)) tryPull(shape.message)
  }

  private def suspendOperations(): Unit = {
    logSubscriptionChange(false)
    setHandler(shape.out, NullDemand)
    stream = None
  }

  private def publishAndPush(pair: (Message, C)): Unit = {
    val (message, context) = pair
    val promise = Promise[(Option[PublishAck], Message, C)]()
    val track = MessageTracker(promise, message, context)
    publish(track)
    push(shape.out, promise.future)
    pendingAck += track
  }

  private def publish(tracker: MessageTracker[C]): Unit = {
    stream match {
      case Some(publisher) =>
        publisher(tracker.message)
          .onComplete(x => publishCompletionCallback.invoke((tracker, x, connectionVersion)))(ExecutionContext.parasitic)
      case None =>
      // will get resubmitted on reconnect
    }
  }

  private def publishCompletion(result: (MessageTracker[C], Try[Option[PublishAck]], Int)): Unit = {
    val (track, tryAck, cachedConnectionVersion) = result
    tryAck match {
      case Success(ack) =>
        pendingAck -= track
        track.promise.trySuccess((ack, track.message, track.context))

      case Failure(cause) if cachedConnectionVersion == connectionVersion =>
        pendingAck -= track
        track.promise.tryFailure(cause)

      case Failure(_) => // requeue message on next connection
    }

    if (isClosed(shape.message) && pendingAck.isEmpty) completeStage()
  }

  protected def name: Option[String] = inheritedAttributes.get[Name].map(_.n)

  protected def logSubscriptionChange(running: Boolean): Unit = {
    val action = if (running) "resuming" else "suspending"
    name match {
      case Some(n) => log.info("{} {} publisher", n, action)
      case None => log.info("{} publisher", action)
    }
  }

  object ConnectionHandler extends InHandler {
    override def onPush(): Unit = {
      grab(shape.protocol) match {
        case Connected(connection) => resumeOperations(connection)
        case Disconnected => suspendOperations()
      }
      pull(shape.protocol)
    }
  }

  object Source extends InHandler {
    override def onPush(): Unit = publishAndPush(grab(shape.message))

    override def onUpstreamFinish(): Unit = if (pendingAck.isEmpty) completeStage()
  }

  object Demand extends OutHandler {
    override def onPull(): Unit = tryPull(shape.message)
  }

  object NullDemand extends OutHandler {
    override def onPull(): Unit = ()
  }
}
