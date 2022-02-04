package com.lolboxen.nats

import akka.util.ReentrantGuard
import io.nats.client.{Message, MessageHandler}

import scala.collection.mutable
import scala.concurrent.Future

class CoreSubscriptionFetch extends SubscriptionFetch with MessageHandler  {

  private val lock = new ReentrantGuard()
  private val notFull = lock.newCondition()

  private var cancelled = false
  private var targetBufferSize = 0
  private val buffer = new mutable.Queue[Message]()

  override def onMessage(msg: Message): Unit = {
    lock.withGuard {
      while (buffer.length >= targetBufferSize && !cancelled) notFull.await()
      if (!cancelled) buffer.enqueue(msg)
    }
  }

  override def cancel(): Unit = lock.withGuard {
    cancelled = true
    notFull.signal()
  }

  override def fetch(batchSize: Int): Future[Seq[Message]] = {
    lock.withGuard {
      targetBufferSize = batchSize
      val result = Seq.fill(batchSize)(if (buffer.isEmpty) None else Some(buffer.dequeue())).flatten
      notFull.signal()
      Future.successful(result)
    }
  }
}

