package io.mewbase.router

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigBeanFactory, ConfigFactory}
import io.mewbase.bson.BsonObject
import io.vertx.core.json.JsonObject
import org.scalatest.{AsyncFlatSpec, FlatSpec}


class RouterSpec extends AsyncFlatSpec {

  val router = HttpEventRouter

  Thread sleep 1000

  implicit val system = ActorSystem("tests")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher


  "The router" should "ping on configured host and port" in {

    val config = ConfigFactory.load()
    val hostname = "localhost"
    val port = 8080

    val ping = s"http://$hostname:$port/ping"

    val publishRequest = Http().singleRequest(HttpRequest(GET, ping)).map { response =>
      assert(response.entity.toString === "pong")
    }
    publishRequest
  }


//  it should "publish on host and port" in  {
//
////  val config = ConfigFactory.load()
////  val hostname = config.getString(HttpEventRouter.HOSTNAME_CONFIG_PATH);
////  val port = config.getInt(HttpEventRouter.PORT_CONFIG_PATH)
////
////  val json = new JsonObject("""{ "name" : "Fred" }""")
////  val bson = new BsonObject(json)
////
////  val entity = bson.encode().getBytes()
////  val ping = s"http://$hostname:$port/publish/bbc7"
////  val publishRequest = Http().singleRequest(HttpRequest(POST, ping, entity = entity)).map { response =>
////    println(response.entity.toString)
////  }
//
//  }

}
