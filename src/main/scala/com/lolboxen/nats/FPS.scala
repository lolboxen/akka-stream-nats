package com.lolboxen.nats

import scala.collection.mutable

class FPS(history: Int) {

  private val duration: mutable.ListBuffer[Long] = mutable.ListBuffer.empty
  private var last: Long = 0L

  def begin(): Unit = last = System.nanoTime()

  def stop(): Unit = {
    val now = System.nanoTime()
    if (last != 0) {
      val delta = now - last
      duration += delta
      duration.dropInPlace((duration.length - history).max(0))
    }
    last = 0
  }

  def hasRate: Boolean = duration.nonEmpty

  def rate(): Double = 1000000000.0 / (duration.sum / duration.length)
}
