package io.mewbase.router

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigBeanFactory, ConfigFactory}
import io.mewbase.bson.BsonObject
import io.vertx.core.json.JsonObject
import org.scalatest.{Assertion, AsyncFlatSpec, FlatSpec}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Failure
import akka.http.scaladsl.testkit.ScalatestRouteTest



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


  it should "publish on host and port" in  {

    val publish = s"http://$hostname:$port/publish/bbc7"

    val bson = new BsonObject(new JsonObject("""{ "name" : "Fred" }"""))
    val entity = bson.encode().getBytes()

    val responseFut = Http().singleRequest(HttpRequest(POST, publish, entity = entity))
    val response= Await.result(responseFut, 5.seconds)
    response.entity.toStrict(5 seconds).map(sten =>
      // check the return matches a valid positive or negative integer
      assert(sten.data.decodeString("UTF-8").matches("-?[0-9]{0,10}") ))
  }


  

}
