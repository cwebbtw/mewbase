package io.mewbase.rest

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import cats.effect.IO
import io.circe.{Json, parser}
import io.mewbase.bson.BsonObject
import io.mewbase.rest.http4s.BsonEntityCodec._
import io.mewbase.rest.http4s.Http4sRestServiceActionVisitor
import io.mewbase.rest.vertx.VertxRestServiceActionVisitor
import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
import io.vertx.core.http.{HttpServer, HttpServerOptions}
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.client.blaze.Http1Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.server.Server
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.threads.threadFactory
import org.http4s.{HttpService, MediaType, Uri}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers, OptionValues}

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

  val restServiceActionVisitor = new Http4sRestServiceActionVisitor[IO]

  val httpService: HttpService[IO] = HttpService[IO] {
    case GET -> Root / "binders" / binderName / documentId =>
      val action = RestServiceAction.retrieveSingleDocument(binderStore, binderName, documentId)
      restServiceActionVisitor.visit(action)

    case req @ POST -> Root / "commands" / commandName =>
      req.as[BsonObject].flatMap { context =>
        val action = RestServiceAction.executeCommand(commandManager, commandName, context)
        restServiceActionVisitor.visit(action)
      }

    case GET -> Root / "binders" / binderName =>
      val action = RestServiceAction.listDocumentIds(binderStore, binderName)
      restServiceActionVisitor.visit(action)

    case GET -> Root / "binders" =>
      val action = RestServiceAction.listBinders(binderStore)
      restServiceActionVisitor.visit(action)

    case req @ POST -> Root / "query" / queryName =>
      req.as[BsonObject].flatMap { context =>
        val action = RestServiceAction.runQuery(queryManager, queryName, context)
        restServiceActionVisitor.visit(action)
      }
  }

  val server =
    BlazeBuilder[IO]
    .bindAny()
    .withExecutionContext(TestExecutionContext)
    .mountService(httpService, "/")

  override def withRunningServer[T](portToResult: Int => T): T = {
    val runningServer: Server[IO] = server.start.unsafeRunSync()
    val result = portToResult(runningServer.address.getPort)
    runningServer.shutdownNow()
    result
  }

}

class VertxRestServiceActionTest extends RestServiceActionTest {

  def configueRouter(router: Router): Unit = {
    router.get("/binders/:binderName/:documentId").handler { rc =>
      val actionVisitor = new VertxRestServiceActionVisitor(rc)
      val action = RestServiceAction.retrieveSingleDocument(binderStore, rc.request().getParam("binderName"), rc.request().getParam("documentId"))
      actionVisitor.visit(action)
    }

    router.post("/commands/:commandName").handler { rc =>
      rc.setAcceptableContentType("application/json")
      val actionVisitor = new VertxRestServiceActionVisitor(rc)
      val bsonBody = actionVisitor.bodyAsBson()
      val action = RestServiceAction.executeCommand(commandManager, rc.request().getParam("commandName"), bsonBody)
      actionVisitor.visit(action)
    }

    router.get("/binders/:binderName").handler { rc =>
      val actionVisitor = new VertxRestServiceActionVisitor(rc)
      val action = RestServiceAction.listDocumentIds(binderStore, rc.request().getParam("binderName"))
      actionVisitor.visit(action)
    }

    router.get("/binders").handler { rc =>
      val actionVisitor = new VertxRestServiceActionVisitor(rc)
      val action = RestServiceAction.listBinders(binderStore)
      actionVisitor.visit(action)
    }

    router.post("/query/:queryName").handler { rc =>
      val actionVisitor = new VertxRestServiceActionVisitor(rc)
      val body = actionVisitor.bodyAsBson()
      val action = RestServiceAction.runQuery(queryManager, rc.request().getParam("queryName"), body)
      actionVisitor.visit(action)
    }
  }

  override def withRunningServer[T](portToResult: Int => T): T = {
    val vertx: Vertx = Vertx.vertx()
    val router: Router = Router.router(vertx)
    router.route.handler(BodyHandler.create)
    configueRouter(router)

    val httpServerOptions = new HttpServerOptions().setPort(0)

    val server: HttpServer = vertx.createHttpServer(httpServerOptions)
    server.requestHandler(router.accept _)

    val countdownLatch = new CountDownLatch(1)
    server.listen((event: AsyncResult[HttpServer]) => countdownLatch.countDown())
    countdownLatch.await()

    val result = portToResult(server.actualPort())

    server.close()
    vertx.close()

    result
  }
}

trait RestServiceActionTest extends FunSuite with Http4sDsl[IO] with Http4sClientDsl[IO] with Matchers with BeforeAndAfterAll with OptionValues with Eventually {

  val binder = StubBinder("testBinder")
  val binderStore = binder.binderStore

  val bson = new BsonObject()
  bson.put("hello", "world")
  binder.put("testDocument", bson)
  binder.put("testDocument2", bson)

  val commandManager = new StubCommandManager()

  val stubQueryResult =
    Map(
      "doc1" -> bson,
      "doc2" -> bson
    )

  val query = StubQuery("testQuery", stubQueryResult)
  val queryManager = query.stubQueryManager()

  def withRunningServer[T](portToResult: Int => T): T

  val client: Client[IO] = Http1Client[IO]().unsafeRunSync()

  test("get a single document") {
    withRunningServer { port =>
      val json = client.expect[Json](s"http://localhost:$port/binders/testBinder/testDocument").unsafeRunSync()
      val jsonObject = json.asObject.value
      jsonObject.keys should have size 1
      jsonObject("hello").value shouldBe Json.fromString("world")
    }
  }

  test("execute command") {
    withRunningServer { port =>
      val requestContext = Json.obj("hello" -> Json.fromString("world"))
      val request = POST(
        Uri.unsafeFromString(s"http://localhost:$port/commands/helloWorld"),
        requestContext,
        `Content-Type`(MediaType.`application/json`)).unsafeRunSync()

      client.status(request).unsafeRunSync().code shouldBe 200

      eventually {
        commandManager.executed should have size (1)
        val (commandName, context) = commandManager.executed.head

        commandName shouldBe "helloWorld"
        context.getString("hello") shouldBe "world"
      }
    }
  }

  test("list document ids") {
    withRunningServer { port =>
      val json = client.expect[Json](s"http://localhost:$port/binders/testBinder").unsafeRunSync()
      val jsonArray = json.asArray.value
      jsonArray shouldBe Vector(Json.fromString("testDocument"), Json.fromString("testDocument2"))
    }
  }

  test("list binders") {
    withRunningServer { port =>
      val json = client.expect[Json](s"http://localhost:$port/binders").unsafeRunSync()
      val jsonArray = json.asArray.value
      jsonArray shouldBe Vector(Json.fromString("testBinder"))
    }
  }

  test("run query") {
    withRunningServer { port =>
      val requestContext = Json.obj("hello" -> Json.fromString("world"))
      val request = POST(
        Uri.unsafeFromString(s"http://localhost:$port/query/testQuery"),
        requestContext,
        `Content-Type`(MediaType.`application/json`)).unsafeRunSync()

      val json = client.expect[Json](request).unsafeRunSync()

      eventually {
        query.executedContexts should have size 1
        query.executedContexts.head shouldBe bson

        val jsonObject = json.asObject.value
        jsonObject.size shouldBe 2
        val bsonAsJson = parser.parse(bson.encodeToString()).toOption.value
        jsonObject("doc1").value shouldBe bsonAsJson
        jsonObject("doc2").value shouldBe bsonAsJson
      }
    }
  }

}


