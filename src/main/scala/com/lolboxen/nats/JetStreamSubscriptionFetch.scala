package com.lolboxen.nats

import io.nats.client.support.NatsJetStreamConstants
import io.nats.client.{JetStreamSubscription, Message}

import java.time.Duration
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.util.Try

class JetStreamSubscriptionFetch(subscription: JetStreamSubscription) extends SubscriptionFetch {

  private val self = this
  private var fetchInProgress = false
  private val executor = Executors.newSingleThreadExecutor()
  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

  override def cancel(): Unit = executor.shutdownNow()

  override def fetch(batchSize: Int): Future[Seq[Message]] = synchronized {
    if (fetchInProgress) throw new IllegalStateException("fetch already in progress")
    fetchInProgress = true

    val promise = Promise[Seq[Message]]()

    Future {
      import scala.jdk.CollectionConverters._
      val validBatchSize = batchSize.min(NatsJetStreamConstants.MAX_PULL_SIZE)
      val result = Try(subscription.fetch(validBatchSize, Duration.ofMillis(100)).asScala.toSeq)
      self.synchronized {
        fetchInProgress = false
        promise.complete(result)
      }
    }

    promise.future
  }
}
