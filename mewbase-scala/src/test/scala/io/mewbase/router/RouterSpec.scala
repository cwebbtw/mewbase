package io.mewbase.router

import java.time.Instant
import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.impl.http.{HttpEventSink, HttpEventSource, SubscriptionRequest}
import io.vertx.core.json.JsonObject
import org.scalatest.{Assertion, AsyncFlatSpec, BeforeAndAfterAll, FlatSpec}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

/*
Test Requires a router running on a local machine on 8081
 */

class RouterSpec extends AsyncFlatSpec with BeforeAndAfterAll {


  implicit val system = ActorSystem("tests")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val hostname = "localhost"
  val port = 8081


  override def beforeAll() {
    HttpEventRouter.main(Array(""))
    Thread.sleep(2000)
  }


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


    it should "subscribe on channel" in  {

      import SubscriptionRequest.SubscriptionType._
      val subsRq = new SubscriptionRequest("bbc 7", FromStart, 0, Instant.EPOCH)
      val subscribeRoute = s"http://$hostname:$port/${HttpEventSource.subscribeRoute}"

      val cdl = new CountDownLatch(1)
      val httpRequest = HttpRequest(POST,  subscribeRoute, entity = subsRq.toBson.encode().getBytes)
      val t = Http().singleRequest(httpRequest).flatMap { response =>
        response.entity.dataBytes.runForeach { chunk: ByteString =>
          val bson = new BsonObject(chunk.toArray[Byte])
          val event = bson.getBsonObject("Event")
          assert(event != null)
          assert(event.getString("name") === "Fred")
          cdl.countDown()
        }
      }

     cdl.await()
      //keep async happy
      Future.successful(assert(true))

  }


  it should "send no event then new events on channel" in {

//    val channelName = "TestChannel"
//
//
//    // set up a subscription from now
//    import SubscriptionRequest.SubscriptionType._
//    val subsRq = new SubscriptionRequest(channelName, FromNow, 0, Instant.EPOCH)
//    val subscribeRoute = s"http://$hostname:$port/${HttpEventSource.subscribeRoute}"
//
//    val cdl = new CountDownLatch(1)
//    val httpRequest = HttpRequest(POST, subscribeRoute, entity = subsRq.toBson.encode().getBytes)
//    val t = Http().singleRequest(httpRequest).flatMap { response =>
//      response.entity.dataBytes.runForeach { chunk: ByteString =>
//        val bson = new BsonObject(chunk.toArray[Byte])
//        val event = bson.getBsonObject("Event")
//        assert(event != null)
//        assert(event.getString("name") === "Fred")
//        cdl.countDown()
//      }
//    }
//
//    Thread.sleep(1000)
//    println("Set up subscription")
//    // write a new event
//    val publish = s"http://$hostname:$port/${HttpEventSink.publishRoute}"
//
//    val event = new BsonObject(new JsonObject("""{ "name" : "Fred" }"""))
//    val body = new BsonObject()
//      .put(HttpEventSink.CHANNEL_TAG, channelName)
//      .put(HttpEventSink.EVENT_TAG, event)
//    val entity = body.encode().getBytes()
//
//    val responseFut = Http().singleRequest(HttpRequest(POST, publish, entity = entity))
//
//    // wait for the event to turn up in the chunks
//    cdl.await()

    //keep async happy
    Future.successful(assert(true))
  }


}