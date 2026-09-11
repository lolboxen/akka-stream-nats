package com.lolboxen.nats

import akka.stream.{Attributes, FlowShape}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client.{Connection, Message}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

abstract class PullSubscriptionLogic[S](executionContext: ExecutionContext,
                                        shape: FlowShape[Protocol, Message],
                                        inheritedAttributes: Attributes)
  extends SubscriptionLogic[Message, S](executionContext, shape, inheritedAttributes) {

  private var isConnected = false
  private var connectionOwner: Option[Connection] = None
  private var disconnectVersion = 0L
  // Includes delivery of the fetched batch, so queued emissions cannot cause
  // another fetch before downstream has consumed the current batch.
  private var isBatchInProgress = false

  private val pullResultCallback = getAsyncCallback[(Long, Try[Seq[Message]])](processResult)

  override protected def onConnected(connection: Connection): Unit = {
    require(connectionOwner.forall(_ eq connection), "A pull subscription cannot resume on a different Connection")
    connectionOwner = Some(connection)
    isConnected = true
    startPullIfNeeded()
  }

  override protected def onDisconnected(): Unit = {
    isConnected = false
    disconnectVersion += 1
  }
  
  override protected def onSubscription(subscription: S): Unit = startPullIfNeeded()

  override def onPull(): Unit = startPullIfNeeded()

  private def startPullIfNeeded(): Unit =
    if (isConnected && isAvailable(shape.out) && !isBatchInProgress) subscription.foreach { owner =>
      isBatchInProgress = true
      val startedVersion = disconnectVersion
      Future(startPull(owner))(executionContext).onComplete { result =>
        pullResultCallback.invoke((startedVersion, result))
      }(ExecutionContext.parasitic)
    }

  // Runs on executionContext; implementations must not access stage state.
  protected def startPull(subscription: S): Seq[Message]

  protected def retryAfterDisconnect(cause: Throwable): Boolean = false

  private def processResult(startedVersion: Long, result: Try[Seq[Message]]): Unit = {
    result match {
      case Success(messages) =>
        // Preserve messages even when the read completes during a disconnect.
        emitMultiple(shape.out, messages.iterator, () => {
          isBatchInProgress = false
          if (!isClosed(shape.out)) startPullIfNeeded()
        })
      case Failure(cause) if retryAfterDisconnect(cause) &&
        (!isConnected || startedVersion != disconnectVersion) =>
        isBatchInProgress = false
        startPullIfNeeded()
      case Failure(cause) => failStage(cause)
    }
  }
}
