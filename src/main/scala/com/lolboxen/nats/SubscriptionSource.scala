package com.lolboxen.nats

import akka.Done
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import io.nats.client.Message

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

class SubscriptionSource(adapter: SubscriptionAdapter) extends GraphStageWithMaterializedValue[SourceShape[Message], Control] {
  case object TimerKey

  val out: Outlet[Message] = Outlet("SubscriberSource.out")
  override def shape: SourceShape[Message] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val stageLogic: GraphStageLogic with Control = new TimerGraphStageLogic(shape) with Control with AdapterListener {

      val shutdownPromise: Promise[Done] = Promise[Done]()
      val fetchCallback: AsyncCallback[Try[Seq[Message]]] = getAsyncCallback(fetchComplete)
      val queue: mutable.Queue[Message] = mutable.Queue.empty
      val fps = new FPS
      var isConnected: Boolean = false
      var fetchInProgress: Boolean = false
      var terminating: Boolean = false

      override def preStart(): Unit = {
        super.preStart()
        adapter.open(AdapterListener(this))
      }

      override def postStop(): Unit = {
        adapter.close()
        shutdownPromise.success(Done)
        super.postStop()
      }

      override def resumeOperations(): Unit = {
        isConnected = true
        scheduleAtFixedRate(TimerKey, 0.seconds, 250.milliseconds)
        pushIfNeeded()
        completeIfNeeded()
      }

      override def suspendOperations(): Unit = {
        cancelTimer(TimerKey)
        isConnected = false
      }

      override def suspendOperationsIndefinitely(cause: Throwable): Unit = failStage(cause)

      override def shutdown(): Unit = {
        getAsyncCallback[Unit] { _ =>
          if (!terminating) {
            terminating = true
            completeIfNeeded()
          }
        }.invoke()
      }

      override def whenShutdown: Future[Done] = shutdownPromise.future

      override def onTimer(timerKey: Any): Unit = ensureQueueHasItems()

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pushIfNeeded()
          completeIfNeeded()
        }
      })

      def ensureQueueHasItems(): Unit =
        if (!terminating && !fetchInProgress && isConnected) {
          implicit val ec: ExecutionContextExecutor = materializer.executionContext
          val rate = if (fps.hasRate) fps.rate().toInt else 1
          val batchSize = if (rate == 0 && queue.length == 1) 1 else rate - queue.length
          if (batchSize > 0) {
            adapter.fetch(batchSize).onComplete(fetchCallback.invoke)
            fetchInProgress = true
          }
        }

      def pushIfNeeded(): Unit = {
        if (isAvailable(out) && !isClosed(out) && queue.nonEmpty) {
          push(out, queue.dequeue())
          fps.mark()
        }
      }

      def completeIfNeeded(): Unit = if (terminating && !fetchInProgress && queue.isEmpty) completeStage()

      def fetchComplete(result: Try[Seq[Message]]): Unit = {
        fetchInProgress = false
        result match {
          case Success(messages) =>
            messages.foreach(queue.enqueue)
            pushIfNeeded()
            completeIfNeeded()
          case Failure(cause) => failStage(cause)
        }
      }
    }

    (stageLogic, stageLogic)
  }
}
