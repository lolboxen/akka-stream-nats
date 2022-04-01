package com.lolboxen.nats

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.lolboxen.nats.ConnectionSource.{Connected, Disconnected}
import io.nats.client.Connection
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Future, Promise}

class ConnectionSourceTest
  extends TestKit(ActorSystem("ConnectionSourceTest"))
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  it should "emit lifecycle events" in {
    val connector = new FakeConnector
    val probe = Source.fromGraph(new ConnectionSource(connector)).runWith(TestSink.probe)
    val connection = mock[Connection]

    whenReady(connector.listener) { listener =>
      listener.onConnect(connection)
      probe.requestNext(Connected(connection))
      listener.onDisconnect()
      probe.requestNext(Disconnected)
      probe.cancel()
    }
  }

  it should "not emit repeating disconnected events" in {
    val connector = new FakeConnector
    val probe = Source.fromGraph(new ConnectionSource(connector)).runWith(TestSink.probe)
    val connection = mock[Connection]

    whenReady(connector.listener) { listener =>
      listener.onConnect(connection)
      probe.requestNext(Connected(connection))
      listener.onDisconnect()
      probe.requestNext(Disconnected)
      listener.onDisconnect()
      probe.request(1).expectNoMessage()
      probe.cancel()
    }
  }

  it should "complete on permanent connection closure" in {
    val connector = new FakeConnector
    val probe = Source.fromGraph(new ConnectionSource(connector)).runWith(TestSink.probe)
    val connection = mock[Connection]

    whenReady(connector.listener) { listener =>
      listener.onConnect(connection)
      probe.requestNext(Connected(connection))
      listener.onClose()
      probe.expectComplete()
    }
  }

  class FakeConnector extends Connector {

    private val _listener: Promise[UnifiedListener] = Promise()

    override def apply(unifiedListener: UnifiedListener): Cancellable = {
      _listener.success(unifiedListener)
      Cancellable.alreadyCancelled
    }

    def listener: Future[UnifiedListener] = _listener.future
  }
}

