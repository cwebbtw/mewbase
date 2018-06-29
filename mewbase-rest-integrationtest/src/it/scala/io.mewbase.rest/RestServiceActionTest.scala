package io.mewbase.rest

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import cats.effect.IO
import io.circe.Json
import io.mewbase.bson.BsonObject
import io.mewbase.rest.http4s.{Http4sRestAdapter, StubBinder}
import io.mewbase.rest.impl.VertxRestServiceAdaptor
import io.mewbase.rest.vertx.VertxRestServiceActionVisitor
import io.vertx.core.http.impl.HttpUtils
import io.vertx.core.{Handler, Vertx}
import io.vertx.core.http.{HttpServer, HttpServerOptions, HttpServerRequest}
import io.vertx.ext.web.{Router, RoutingContext}
import org.http4s.{HttpService, Request, Response}
import org.http4s.Status.Successful
import org.http4s.client.Client
import org.http4s.client.blaze.Http1Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.threads.threadFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers, OptionValues}
import org.http4s.circe._
import org.http4s.server.Server

import scala.concurrent.ExecutionContext

class Http4sRestServiceActionTest extends RestServiceActionTest {

  def newDaemonPool(
                     name: String,
                     min: Int = 4,
                     cpuFactor: Double = 3.0,
                     timeout: Boolean = false): ThreadPoolExecutor = {
    val cpus = Runtime.getRuntime.availableProcessors
    val exec = new ThreadPoolExecutor(
      math.max(min, cpus),
      math.max(min, (cpus * cpuFactor).ceil.toInt),
      10L,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory(i => s"$name-$i", daemon = true)
    )
    exec.allowCoreThreadTimeOut(timeout)
    exec
  }

  val TestExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(newDaemonPool("http4s-spec", timeout = true))

  val requestMapper: java.util.function.Function[Request[IO], IO[RestServiceAction[_]]] = {
    case GET -> Root / "binders" / binderName / documentId =>
      IO.pure(new RestServiceAction.RetrieveSingleDocument(binderStore, binderName, documentId))
  }

  val service: HttpService[IO] = new Http4sRestAdapter[IO]().adapt(requestMapper)

  val server: Server[IO] =
    BlazeBuilder[IO]
    .bindAny()
    .withExecutionContext(TestExecutionContext)
    .mountService(service, "/")
    .start
    .unsafeRunSync()

  override def start(): Int =
    server.address.getPort

  override def stop(): Unit =
    server.shutdownNow()

}

class VertxRestServiceActionTest extends RestServiceActionTest {

  val vertx: Vertx = Vertx.vertx()
  val options: HttpServerOptions = new HttpServerOptions().setPort(9081)
  val server: HttpServer = vertx.createHttpServer(options)

  val router: Router = Router.router(vertx)

  router.get("/binders/:binderName/:documentId").handler { rc =>
    val actionVisitor = VertxRestServiceActionVisitor.actionVisitor(rc.response())
    val action = new RestServiceAction.RetrieveSingleDocument(binderStore, rc.request().getParam("binderName"), rc.request().getParam("documentId"))
    action.visit(actionVisitor)
  }

  override def start(): Int = {
    server.requestHandler(router.accept _)
    server.listen().actualPort()
  }

  override def stop(): Unit =
    server.close()
}

trait RestServiceActionTest extends FunSuite with Http4sDsl[IO] with Http4sClientDsl[IO] with Matchers with BeforeAndAfterAll with OptionValues {

  val binder = StubBinder("testBinder")
  val binderStore = binder.binderStore

  val bson = new BsonObject()
  bson.put("hello", "world")
  binder.put("testDocument", bson)

  def start(): Int
  def stop(): Unit

  var port = 0

  override def beforeAll(): Unit =
    port = start()

  override def afterAll(): Unit =
    stop()

  val client: Client[IO] = Http1Client[IO]().unsafeRunSync()

  test("get a single document") {
    val json = client.expect[Json](s"http://localhost:$port/binders/testBinder/testDocument").unsafeRunSync()
    val jsonObject = json.asObject.value
    jsonObject.keys should have size 1
    jsonObject("hello").value shouldBe Json.fromString("world")
  }

}


