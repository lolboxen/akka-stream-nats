package com.lolboxen.nats

import akka.actor.Cancellable
import io.nats.client.{Nats, Options}

import java.time.Duration
import java.util.concurrent.ThreadLocalRandom
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait Connector {
  def apply(unifiedListener: UnifiedListener): Cancellable
}

class NatsConnector(optionsBuilder: Options.Builder) extends Connector {
  def apply(unifiedListener: UnifiedListener): Cancellable = {
    val runnable: Runnable with Cancellable = new Runnable with Cancellable {

      private val unifiedListenerAdapter = new UnifiedListenerAdapter(unifiedListener)

      @volatile private var _cancel: Boolean = false
      @volatile private var _cancelled: Boolean = false

      @volatile private var _thread: Option[Thread] = None

      val options: Options =
        optionsBuilder
          .connectionListener(unifiedListenerAdapter)
          .errorListener(unifiedListenerAdapter)
          .build()

      override def run(): Unit = {
        _thread = Some(Thread.currentThread())
        connectLoop(0)
        _cancelled = _cancel
      }

      @tailrec def connectLoop(attempts: Int): Unit =
        if (!_cancel) {
          Try {
            Nats.connect(options)
          } match {
            case Success(_) =>
            case Failure(_) =>
              val nextAttempts = attempts + 1
              if (!hasMoreAttempts(nextAttempts, options)) unifiedListener.onClose()
              else
                getReconnectDelay(nextAttempts, options) match {
                  case Some(delay) =>
                    if (sleep(delay)) connectLoop(nextAttempts)
                    else unifiedListener.onClose()
                  case None =>
                    unifiedListener.onClose()
                }
          }
        }

      def sleep(duration: Duration): Boolean = {
        try {
          Thread.sleep(duration.toMillis)
          true
        }
        catch {
          case _: Throwable => false
        }
      }

      override def cancel(): Boolean = {
        _thread.foreach(_.interrupt())
        _cancel = true
        true
      }

      override def isCancelled: Boolean = _cancelled
    }
    new Thread(runnable).start()
    runnable
  }

  private def getReconnectDelay(attempts: Int, options: Options): Option[Duration] =
    Option(options.getReconnectDelayHandler)
      .flatMap(x => Option(x.getWaitTime(attempts)))
      .orElse {
        Option(options.getReconnectWait)
          .map { waitTime =>
            val jitter = (
              if (options.isTLSRequired) Option(options.getReconnectJitterTls)
              else Option(options.getReconnectJitter)
              ).getOrElse(Duration.ZERO).toMillis
            waitTime.plusMillis(ThreadLocalRandom.current().nextLong(jitter))
          }
      }

  private def hasMoreAttempts(attempts: Int, options: Options): Boolean =
    options.getMaxReconnect == -1 || attempts < options.getMaxReconnect
}
