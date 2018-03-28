package io.mewbase.router

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.stream.ActorMaterializer

import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.impl.http.HttpEventSink
import io.vertx.core.json.JsonObject
import org.scalatest.{Assertion, AsyncFlatSpec, FlatSpec}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import scala.language.postfixOps

/*
Test Requires a router running on a local machine on 8081
 */

class RouterSpec extends AsyncFlatSpec {


  implicit val system = ActorSystem("tests")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val hostname = "localhost"
  val port = 8081


  "The router" should "ping on configured host and port" in {

    val ping = s"http://$hostname:$port/ping"

    val responseFut  = Http().singleRequest(HttpRequest(GET, ping))
    val response= Await.result(responseFut, 5.seconds)
    response.entity.toStrict(5 seconds).map(sten => assert(sten.data.decodeString("UTF-8") == "pong"))

  }


  it should "publish on channel" in  {

    val publish = s"http://$hostname:$port/${HttpEventSink.publishRoute}"

    val event  = new BsonObject(new JsonObject("""{ "name" : "Fred" }"""))
    val body = new BsonObject()
        .put(HttpEventSink.CHANNEL_TAG, "bbc 7")
        .put(HttpEventSink.EVENT_TAG, event)
    val entity = body.encode().getBytes()

    val responseFut = Http().singleRequest(HttpRequest(POST, publish, entity = entity))
    val response= Await.result(responseFut, 5.seconds)
    response.entity.toStrict(5 seconds).map(sten =>
      // check the return matches a valid positive or negative integer
      assert(sten.data.decodeString("UTF-8").matches("-?\\d+") ))
  }


//  it should "subscribe on channel" in  {
//
//    val publish = s"http://$hostname:$port/publish/" + URLEncoder.encode("bbc 7", "UTF-8")
//
//
//  }


}
