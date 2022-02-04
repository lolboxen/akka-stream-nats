package com.lolboxen.nats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FPSTest extends AnyFlatSpec with Matchers {
  it should "measure rate per second" in {
    val fps = new FPS(100)
    for(_ <- 1 to 100) {
      fps.begin()
      Thread.sleep(1)
      fps.stop()
    }
    fps.rate() should (be > 700.0 and be < 1000.0)
  }
}
