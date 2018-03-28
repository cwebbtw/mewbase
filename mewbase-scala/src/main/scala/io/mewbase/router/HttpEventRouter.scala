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
import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.EventSink
import io.mewbase.eventsource.impl.http.HttpEventSink
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global


object HttpEventRouter extends App
{

  implicit val system = ActorSystem("server")
  implicit val materializer = ActorMaterializer()

  val PORT_CONFIG_PATH = "mewbase.http.router.port"


  val log = LoggerFactory.getLogger(getClass.getName)

  val sink = EventSink.instance()


  // Store a killSwitch associated with each subscription
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
          log.debug(s"get - ping")
          complete("pong")
        }
      }



  val publishRoute =
    post {
      path(HttpEventSink.publishRoute ) {
          entity(as[Array[Byte]]) { body =>
            val bson = new BsonObject(body)
            val channelName = bson.getString(HttpEventSink.CHANNEL_TAG)
            val event = bson.getBsonObject(HttpEventSink.EVENT_TAG)
            val eventNumber = sink.publishSync(channelName, event)
            log.debug(s"post - ${HttpEventSink.publishRoute} $channelName" )
            complete(HttpEntity(eventNumber.toString()))
        }
      }
    }


  val subscribeRoute =
    post {
      path ("subscribe"  ) {
        entity(as[Array[Byte]]) { body =>
          val bson = new BsonObject(body)
          val channelName = bson.getString(HttpEventSink.CHANNEL_TAG)
          val source = createStringSource(channelName).map {
            str => ChunkStreamPart(s"Channel : $channelName EventNumber :$str")
          }
        complete(HttpEntity.Chunked(ContentTypes.`application/octet-stream`, source))
        }
      }
    }


  val unsubscribeRoute =
    post {
      path ("unsubscribe" / Segment ) { subscriptionID =>
        val responseMsg = "unsubscribe :" + subscriptionID
        logRequest(responseMsg) {
          killSwitches.get(subscriptionID).foreach { kSwitch =>
            kSwitch.complete(Try { () => () })
            killSwitches.remove(subscriptionID)
          }
          complete(responseMsg)
        }
      }
    }


  val allRoutes =  { pingRoute ~ publishRoute ~ subscribeRoute ~ unsubscribeRoute }

  val config = ConfigFactory.load()
  val port = config.getInt(PORT_CONFIG_PATH)
  val interfaces = "0.0.0.0"

  val serverSource = Http().bindAndHandle(allRoutes, interfaces, port)

  log.info(s"Server Started on Port :$port")


}
