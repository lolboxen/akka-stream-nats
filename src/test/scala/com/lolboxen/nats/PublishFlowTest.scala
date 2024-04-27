package com.lolboxen.nats

import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl.{GraphDSL, Keep, Source}
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import com.lolboxen.nats.ConnectionSource.{Connected, Protocol}
import com.lolboxen.nats.Publisher.Factory
import io.nats.client.{Connection, JetStream, JetStreamOptions, Message}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PublishFlowTest
  extends TestKit(ActorSystem("PublishFlowTest"))
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  it should "publish messages to jetstream when connected" in {
    val connection = mock[Connection]
    val jetStream = mock[JetStream]
    val message = mock[Message]
    (connection.jetStream(_: JetStreamOptions)).expects(*).once().returning(jetStream)
    (jetStream.publishAsync(_: Message)).expects(*).returning(null)

    val (msgPub, conPub, sub) = runningGraphForTesting(Publisher.jetStream(JetStreamOptions.defaultOptions()))

    conPub.sendNext(Connected(connection))
    msgPub.sendNext((message, ()))
    sub.requestNext()
    msgPub.sendComplete()
  }

  it should "publish messages to core when connected" in {
    val connection = mock[Connection]
    val message = mock[Message]
    (connection.publish(_: Message)).expects(*)

    val (msgPub, conPub, sub) = runningGraphForTesting(Publisher.core)

    conPub.sendNext(Connected(connection))
    msgPub.sendNext((message, ()))
    sub.requestNext()
    msgPub.sendComplete()
  }

  private def runningGraphForTesting(publisherFactory: Factory) = {
    val ((msgPub, conPub), sub) = TestSource[(Message, Unit)]()
      .viaMat(publishingGraph(TestSource[Protocol](), publisherFactory))(Keep.both)
      .toMat(TestSink())(Keep.both)
      .run()

    (msgPub, conPub, sub)
  }

  private def publishingGraph(connectionSource: Source[Protocol, TestPublisher.Probe[Protocol]],
                              publisherFactory: Factory) =
    GraphDSL.createGraph(connectionSource) { implicit builder => connection =>
      import GraphDSL.Implicits._
      val publish = builder.add(new PublishFlow[Unit](publisherFactory))
      connection.out ~> publish.protocol
      FlowShape(publish.message, publish.out)
    }
}
