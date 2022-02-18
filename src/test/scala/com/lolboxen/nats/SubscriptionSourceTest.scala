package com.lolboxen.nats

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import io.nats.client.Message
import io.nats.client.impl.NatsMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.{ExecutionContext, Future, Promise}

class SubscriptionSourceTest extends AnyFlatSpec with Matchers {

  implicit val system: ActorSystem = ActorSystem()

  it should "open subscription on start and close on termination" in {
    val testAdapter = new TestSubscriptionAdapter

    val subscription = Source.fromGraph(new SubscriptionSource(testAdapter))
      .toMat(Sink.ignore)(Keep.left)
      .run()

    Thread.sleep(100)
    testAdapter.ensureOpened()
    subscription.shutdown()
    testAdapter.fulfillFetch(Nil)
    Thread.sleep(100)
    testAdapter.ensureClosed()
  }

  it should "poll messages from subscription and push downstream" in {
    val testAdapter = new TestSubscriptionAdapter

    val probe = Source.fromGraph(new SubscriptionSource(testAdapter))
      .toMat(TestSink.probe)(Keep.right)
      .run()

    probe.ensureSubscription()
    testAdapter.fulfillFetch(Seq(makeMessage("1"), makeMessage("2")))
    probe.requestNext().getSubject shouldBe "1"
    probe.requestNext().getSubject shouldBe "2"
//    testAdapter.expectBatchSize(1, 1)
    testAdapter.fulfillFetch(Seq(makeMessage("3"), makeMessage("4")))
    probe.requestNext().getSubject shouldBe "3"
    probe.requestNext().getSubject shouldBe "4"
//    testAdapter.expectBatchSize(50, 1000)
    testAdapter.fulfillFetch(Seq(makeMessage("5"), makeMessage("6")))
    probe.requestNext().getSubject shouldBe "5"
    probe.requestNext().getSubject shouldBe "6"
//    testAdapter.expectBatchSize(50, 1000)
//    testAdapter.expectBatchSize(50, 1000)
  }

  private def makeMessage(id: String): Message = NatsMessage.builder().subject(id).build()

  class TestSubscriptionAdapter extends SubscriptionAdapter {
    private var didOpen: Boolean = false
    private var didClose: Boolean = false
    private val fetchQueue = new ConcurrentLinkedQueue[Seq[Message]]()
    private val batchSizes = new ConcurrentLinkedQueue[Int]()
    private var fetchInProgress: Boolean = false

    override def open(listener: AdapterListener): Unit = {
      didOpen = true
      listener.resumeOperations()
    }

    override def close(): Unit = didClose = true

    override def fetch(batchSize: Int): Future[Seq[Message]] = {
      if (fetchInProgress) throw new Exception("fetch in progress")
      batchSizes.add(batchSize)
      fetchInProgress = true
      val promise = Promise[Seq[Message]]()
      Future {
        while (fetchQueue.isEmpty) Thread.sleep(1)
        val messages = fetchQueue.poll()
        fetchInProgress = false
        promise.success(messages)
      }(scala.concurrent.ExecutionContext.global)
      promise.future
    }

    def ensureOpened(): Unit = assert(didOpen)

    def ensureClosed(): Unit = assert(didClose)

    def fulfillFetch(messages: Seq[Message]): Unit = fetchQueue.add(messages)

    def expectBatchSize(lower: Int, upper: Int): Unit = {
      val batchSize = batchSizes.poll()
      assert(batchSize >= lower && batchSize <= upper)
    }
  }
}
