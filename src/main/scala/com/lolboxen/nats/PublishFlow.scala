package com.lolboxen.nats

import akka.Done
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream._
import akka.stream.stage._
import io.nats.client.Message
import io.nats.client.api.PublishAck

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

class PublishFlow[Context](adapter: PublishAdapter) extends GraphStage[FlowShape[(Message, Context), Future[(PublishAck, Message, Context)]]] {
  val in: Inlet[(Message, Context)] = Inlet("PublishFlow.in")
  val out: Outlet[Future[(PublishAck, Message, Context)]] = Outlet("PublishFlow.out")
  override val shape: FlowShape[(Message, Context), Future[(PublishAck, Message, Context)]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PublishFlowStageLogic(this, inheritedAttributes, adapter)
}

class PublishFlowStageLogic[Context](stage: PublishFlow[Context],
                                     inheritedAttributes: Attributes,
                                     adapter: PublishAdapter)
  extends GraphStageLogic(stage.shape)
    with StageLogging
    with AdapterListener {

  val publishCompletionCallback: AsyncCallback[(Promise[(PublishAck, Message, Context)], (Message, Context), Try[Option[PublishAck]], Int)] = getAsyncCallback(publishCompletion)
  val pendingAck: ListBuffer[(Promise[(PublishAck, Message, Context)], Message, Context)] = ListBuffer.empty
  var finalizer: Option[Try[Done]] = None
  // messages that were delivered but not acked can fail as much as two minutes later across a reconnect event
  // connection version will filter out those publish promises
  var connectionVersion: Int = 0
  private lazy val decider: Decider =
    inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

  setHandler(stage.in, Source)
  setHandler(stage.out, NullDemand)

  override def preStart(): Unit = {
    super.preStart()
    adapter.open(AdapterListener(this))
  }

  override def postStop(): Unit = {
    adapter.close()
    super.postStop()
  }

  override def resumeOperations(): Unit = {
    connectionVersion += 1
    setHandler(stage.out, Demand)
    pendingAck.foreach { case (promise, message, context) => publish(promise, (message, context))}
    if (isAvailable(stage.out) && !hasBeenPulled(stage.in)) tryPull(stage.in)
  }

  override def suspendOperations(): Unit = setHandler(stage.out, NullDemand)

  override def suspendOperationsIndefinitely(cause: Throwable): Unit = failStage(cause)

  def publishCompletion(result: (Promise[(PublishAck, Message, Context)], (Message, Context), Try[Option[PublishAck]], Int)): Unit = {
    val (promise, (message, context), tryAck, cachedConnectionVersion) = result
    tryAck match {
      case Success(Some(ack)) =>
        pendingAck.dropWhileInPlace(_ == (promise, message, context))
        promise.success((ack, message, context))
        finalizeIfNeeded()

      case Success(None) => // publish disconnected thus do nothing and republish on connect

      case Failure(cause) if cachedConnectionVersion == connectionVersion =>
        decider(cause) match {
          case Supervision.Stop => failStage(cause)
          case _ => promise.failure(cause)
        }

      case _ =>
    }
  }

  def publishAndPush(pair: (Message, Context)): Unit = {
    val promise = Promise[(PublishAck, Message, Context)]()
    publish(promise, pair)
    push(stage.out, promise.future)
    pendingAck.addOne((promise, pair._1, pair._2))
  }

  def publish(promise: Promise[(PublishAck, Message, Context)], pair: (Message, Context)): Unit = {
    implicit val ec: ExecutionContextExecutor = materializer.executionContext
    val (message, _) = pair
    adapter(message).onComplete(x => publishCompletionCallback.invoke((promise, pair, x, connectionVersion)))
  }

  def finalizeIfNeeded(): Unit =
    if (isClosed(stage.in) && pendingAck.isEmpty)
      finalizer match {
        case Some(Success(_)) => completeStage()
        case Some(Failure(cause)) => failStage(cause)
        case None => failStage(new IllegalStateException("Stage completed abnormally"))
      }

  object Source extends InHandler {
    override def onUpstreamFailure(ex: Throwable): Unit = {
      finalizer = Some(Failure(ex))
      finalizeIfNeeded()
    }

    override def onUpstreamFinish(): Unit = {
      finalizer = Some(Success(Done))
      finalizeIfNeeded()
    }

    override def onPush(): Unit = publishAndPush(grab(stage.in))
  }

  object Demand extends OutHandler {
    override def onPull(): Unit = tryPull(stage.in)
  }

  object NullDemand extends OutHandler {
    override def onPull(): Unit = ()
  }
}
