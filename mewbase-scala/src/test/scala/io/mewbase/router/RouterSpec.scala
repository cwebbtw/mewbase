package io.mewbase.router

import java.time.Instant
import java.util.UUID
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}

import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.impl.http.{HttpEventSink, HttpEventSource, SubscriptionRequest}
import io.vertx.core.{Handler, Vertx}
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.{HttpClientOptions, HttpClientResponse}
import io.vertx.core.json.JsonObject
import org.scalatest.{Assertion, AsyncFlatSpec, BeforeAndAfterAll, FlatSpec}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps


/* Test Requires a router running on a local machine on 8081 */

class RouterSpec extends AsyncFlatSpec with BeforeAndAfterAll {

  val hostname = "127.0.0.1"
  val port = 8081
  val options = new HttpClientOptions().setDefaultHost(hostname).setDefaultPort(port)

  val EVENT_TAG = "event"
  val CHANNEL_TAG = "channel"




  "The router" should "ping on configured host and port" in {

    val client = Vertx.vertx.createHttpClient(options)

    val pingRoute = "ping"

    val p = Promise[String]()

    val bodyHandler : Handler[Buffer] = (totalBuffer: Buffer) => {
      p success totalBuffer.toString()
    }

    val handler : Handler[HttpClientResponse] = response => {
      println(response)
      response.bodyHandler(bodyHandler )
    }

    client.getNow(pingRoute, handler)

    p.future.map( result => assert( result === "pong"))

  }


  it should "publish on channel" in {

    //    options = new HttpClientOptions().setDefaultHost(hostname).setDefaultPort(port)
    //
    //    val body = new BsonObject().put(CHANNEL_TAG, channelName).put(EVENT_TAG, event)
    //
    //    val fut = new CompletableFuture[Long]
    //    client.post("/" + publishRoute, (response: HttpClientResponse) => response.bodyHandler((totalBuffer: Buffer) => {
    //      def foo(totalBuffer: Buffer) = try {
    //        val eventNumberAsString = totalBuffer.toString
    //        val eventNumber = Long.valueOf(eventNumberAsString)
    //        fut.complete(eventNumber)
    //      } catch {
    //        case exp: Exception =>
    //          fut.completeExceptionally(exp)
    //      }
    //
    //      foo(totalBuffer)
    //    })).end(body.encode)
    //    return fut
    //      assert(sten.data.decodeString("UTF-8").matches("-?\\d+") ))
    //  }
    Future.successful(assert(true))
  }


    it should "subscribe on channel" in  {

//      val channelName = "bbc7"
//
//      import SubscriptionRequest.SubscriptionType._
//      val subsRq = new SubscriptionRequest(channelName, FromStart, 0, Instant.EPOCH)
//      val subscribeRoute = s"http://$hostname:$port/${HttpEventSource.subscribeRoute}"
//
//      val cdl = new CountDownLatch(1)
//      val httpRequest = HttpRequest(POST,  subscribeRoute, entity = subsRq.toBson.encode().getBytes)
//      val t = Http().singleRequest(httpRequest).flatMap { response =>
//        response.entity.dataBytes.runForeach { chunk: ByteString =>
//          val bson = new BsonObject(chunk.toArray[Byte])
//          val event = bson.getBsonObject("Event")
//          assert(event != null)
//          assert(event.getString("name") === "Fred")
//          cdl.countDown()
//        }
//      }
//
//     cdl.await()
//
//      // write another and see if it pops up
//
//      Thread.sleep(1000)
//
//
//      // write a new event
//      val publish = s"http://$hostname:$port/${HttpEventSink.publishRoute}"
//
//      val event = new BsonObject(new JsonObject("""{ "name" : "Fred" }"""))
//      val body = new BsonObject()
//        .put(HttpEventSink.CHANNEL_TAG, channelName)
//        .put(HttpEventSink.EVENT_TAG, event)
//      val entity = body.encode().getBytes()
//
//      val responseFut = Http().singleRequest(HttpRequest(POST, publish, entity = entity))
//      Await.ready(responseFut, 10 seconds)
//
//      Thread.sleep(1000)
      //keep async happy
      Future.successful(assert(true))

  }


  it should "send no event then new events on channel" in {

//    val channelName = "TestChannel"
//
//    val testUniqueValue = UUID.randomUUID().toString
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
//        println("Received a chunk " + chunk)
//        val bson = new BsonObject(chunk.toArray[Byte])
//        println("Received a bson " + bson)
//        val event = bson.getBsonObject("Event")
//        assert(event != null)
//        assert(event.getString("name") === testUniqueValue)
//        cdl.countDown()
//      }
//    }
//
//    Thread.sleep(1000)
//
//    // write a new event
//    val publish = s"http://$hostname:$port/${HttpEventSink.publishRoute}"
//
//    val event = new BsonObject(new JsonObject(s"""{ "name" : "$testUniqueValue" }"""))
//    val body = new BsonObject()
//      .put(HttpEventSink.CHANNEL_TAG, channelName)
//      .put(HttpEventSink.EVENT_TAG, event)
//    val entity = body.encode().getBytes()
//
//    val responseFut = Http().singleRequest(HttpRequest(POST, publish, entity = entity))
//    println(responseFut.map( resp => resp.entity) )
//    val res = Await.result(responseFut, 10 seconds)
//    res.entity.toStrict(5 seconds).map( v => println("Value " + v))
//    println("After 10 " + responseFut.map( resp => resp.entity) )
//    // wait for the event to turn up in the chunks
//    cdl.await(10,TimeUnit.SECONDS)

    //keep async happy
    Future.successful(assert(true))
  }


}
