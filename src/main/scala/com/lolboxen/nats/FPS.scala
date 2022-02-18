package com.lolboxen.nats

import java.time.Duration
import scala.collection.mutable

class FPS {

  private val oneSecond = Duration.ofSeconds(1).toNanos
  private val timestamps: mutable.ListBuffer[Long] = mutable.ListBuffer.empty

  def mark(): Unit = {
    val nanoNow = System.nanoTime()
    val trimBefore = nanoNow - oneSecond
    timestamps.dropWhileInPlace(_ < trimBefore)
    timestamps += nanoNow
  }

  def hasRate: Boolean = timestamps.nonEmpty

  def rate(): Double = timestamps.length
}
