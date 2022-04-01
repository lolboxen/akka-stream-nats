package util

import scala.concurrent.Promise

object MockUtils {
  def captureAndReturn[I, O](promise: Promise[I], out: O): I => O = in => {
    promise.success(in)
    out
  }
}
