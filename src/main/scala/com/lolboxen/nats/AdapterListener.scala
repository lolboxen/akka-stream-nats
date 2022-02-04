package com.lolboxen.nats

import akka.stream.stage.GraphStageLogic

trait AdapterListener {
  def resumeOperations(): Unit

  def suspendOperations(): Unit

  def suspendOperationsIndefinitely(cause: Throwable): Unit
}

object AdapterListener {
  def apply(logic: GraphStageLogic with AdapterListener): AdapterListener = {
    val resumePublishingCb = logic.getAsyncCallback[Unit](_ => logic.resumeOperations())
    val suspendPublishingCb = logic.getAsyncCallback[Unit](_ => logic.suspendOperations())
    val suspendPublishingIndefinitelyCb = logic.getAsyncCallback(logic.suspendOperationsIndefinitely)
    new AdapterListener {
      override def resumeOperations(): Unit = resumePublishingCb.invoke(())

      override def suspendOperations(): Unit = suspendPublishingCb.invoke(())

      override def suspendOperationsIndefinitely(cause: Throwable): Unit = suspendPublishingIndefinitelyCb.invoke(cause)
    }
  }
}