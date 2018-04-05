package io.mewbase.router



import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import io.mewbase.router.HttpEventRouter.log


case class SubscriptionChunkSource(pushPull : SubscriptionPushPull) extends GraphStage[SourceShape[ChunkStreamPart]] {

  // Define the (sole) output port of this stage
  val out: Outlet[ChunkStreamPart] = Outlet("ChunkSource")

  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: SourceShape[ChunkStreamPart] = SourceShape(out)

  // This is where the actual (possibly stateful) logic will live
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      setHandler(out, new OutHandler {
        override def onPull() : Unit = {
           push(out, pushPull.pull )
        }

        override def onDownstreamFinish(): Unit = {
          super.onDownstreamFinish()
          pushPull.subscription.close()
          log.info("Cleaning up ChunkSource subscription")
          complete(out)
        }
      })
    }



}
