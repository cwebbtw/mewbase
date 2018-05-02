package io.mewbase.router



import io.vertx.core.{Handler, Vertx}
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.{HttpClientOptions, HttpClientResponse}
import org.scalatest.{Assertion, AsyncFlatSpec, BeforeAndAfterAll, FlatSpec}

import scala.concurrent.{Await, Future, Promise}
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

    val pingRoute = "/ping"

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


}
