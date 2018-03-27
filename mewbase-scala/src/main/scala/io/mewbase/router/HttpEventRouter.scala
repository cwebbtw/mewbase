package io.mewbase.router


import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{entity, _}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global


object HttpEventRouter extends App
{

  implicit val system = ActorSystem("server")
  implicit val materializer = ActorMaterializer()

  val HOSTNAME_CONFIG_PATH = "mewbase.http.router.hostname"
  val PORT_CONFIG_PATH = "mewbase.http.router.port"


  // Store a killSwitch associated with each
  val killSwitches = mutable.Map[String, Promise[Unit]]()


  class StringSource(id : UUID, killSwitch : Future[Unit]) extends GraphStage[SourceShape[String]] {
    // Define the (sole) output port of this stage
    val out: Outlet[String] = Outlet("StringSource")

    // Define the shape of this stage, which is SourceShape with the port we defined above
    override val shape: SourceShape[String] = SourceShape(out)


    // This is where the actual (possibly stateful) logic will live
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var counter = 0

        override def preStart(): Unit = {
          val callback = getAsyncCallback[Unit] { (_) =>
            completeStage()
            println("Cleaning up resources")
          }
          killSwitch.foreach(callback.invoke)
        }

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (counter == 0) push(out, id.toString())
            else push(out, counter.toString())
            counter += 1
            Thread.sleep(1000)
          }
        })
      }
  }


  def createStringSource(channelName : String) : Source[String,NotUsed]  = {
    val id = UUID.randomUUID
    val killSwitch = Promise[Unit]()
    killSwitches put(id.toString, killSwitch)
    Source.fromGraph( new StringSource(id, killSwitch.future ))
  }


  val pingRoute =
    get {
      path ( "ping" ) {
        println("pinged")
        complete("pong")
      }
    }

  val publishRoute =
    post {
      path("publish" / Segment ) { channelName =>
        entity(as[String]) { json =>
          println(channelName + " published " + json)
          complete(HttpEntity(0L toString() ))
        }
      }
    }

  val subscribeRoute =
    post {
      path ("subscribe" / Segment ) { channelName =>
        println("subscribe to " + channelName)
        val source = createStringSource(channelName).map {
          str => ChunkStreamPart(s"Channel : $channelName EventNumber :$str")
        }
        complete(HttpEntity.Chunked(ContentTypes.`application/json`, source))
      }
    }

  val unsubscribeRoute =
    post {
      path ("unsubscribe" / Segment ) {  subscriptionID =>
        println("unsubscribing " + subscriptionID)
        killSwitches.get(subscriptionID).foreach {  kSwitch =>
          kSwitch.complete( Try { () => () }  )
          killSwitches.remove(subscriptionID)
        }
        complete( "Unsubscribe:"+subscriptionID )
      }
    }

  val allRoutes =  { pingRoute ~ publishRoute ~ subscribeRoute ~ unsubscribeRoute }

  val config = ConfigFactory.load()
  val interfaces = "0.0.0.0"
  val port = config.getInt(PORT_CONFIG_PATH)

  val serverSource = Http().bindAndHandle(allRoutes, interfaces, port)

  println (s"Server Started on Port :$port")

}
