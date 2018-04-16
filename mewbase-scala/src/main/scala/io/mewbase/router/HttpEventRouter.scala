package io.mewbase.router




import akka.actor.ActorSystem
import akka.http.scaladsl.Http

import akka.stream._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{entity, _}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.{EventSink, EventSource}
import io.mewbase.eventsource.impl.http.{HttpEventSink, HttpEventSource, SubscriptionRequest}
import org.slf4j.LoggerFactory




object HttpEventRouter extends App
{

  implicit val system = ActorSystem("server")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher


  val PORT_CONFIG_PATH = "mewbase.http.router.port"

  val log = LoggerFactory.getLogger(getClass.getName)

  val sink = EventSink.instance()
  val source = EventSource.instance()


  val pingRoute =
    get {
      path ( "ping" ) {
          log.info(s"get - ping")
          complete("pong")
        }
      }


  val publishRoute =
    post {
      path(HttpEventSink.publishRoute ) {
          entity(as[Array[Byte]]) { body => {
            val bson = new BsonObject(body)
            val channelName = bson.getString(HttpEventSink.CHANNEL_TAG)
            val event = bson.getBsonObject(HttpEventSink.EVENT_TAG)
            val eventNumber = sink.publishSync(channelName, event)
            log.info(s"post - ${HttpEventSink.publishRoute} $channelName")
            complete(HttpEntity(eventNumber.toString()))
          }
        }
      }
    }


  val subscribeRoute =
    post {
      path (HttpEventSource.subscribeRoute ) {
        entity(as[Array[Byte]]) { body => {
          val bson = new BsonObject(body)
          val subReq = new SubscriptionRequest(bson)
          val pushPull = SubscriptionPushPull(source,subReq)
          val chunkGraph = SubscriptionChunkSource(pushPull)
          val chunkSource = Source.fromGraph(chunkGraph)
          log.info(s"post - ${HttpEventSource.subscribeRoute} $subReq" )
          complete(HttpEntity.Chunked(ContentTypes.`application/octet-stream`, chunkSource ))
          }
        }
      }
    }



  val allRoutes =  { pingRoute ~ publishRoute ~ subscribeRoute }

  val config = ConfigFactory.load()
  val port = config.getInt(PORT_CONFIG_PATH)
  val interfaces = "0.0.0.0"

  val serverSource = Http().bindAndHandle(allRoutes, interfaces, port)

  log.info(s"Server Started on Port :$port")


}
