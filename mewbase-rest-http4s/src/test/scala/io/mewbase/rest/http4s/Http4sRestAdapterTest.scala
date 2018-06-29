package io.mewbase.rest.http4s

import java.util.stream.Collectors

import cats.effect.IO
import io.mewbase.binders.BinderStore
import io.mewbase.bson.{BsonArray, BsonObject}
import io.mewbase.rest.RestServiceAction
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpService, Method, Request, Uri}
import org.scalatest.{FunSuite, Matchers, OptionValues}
import org.http4s.implicits._
import org.http4s.client.dsl.Http4sClientDsl
import BsonEntityCodec._

class Http4sRestAdapterTest extends FunSuite with Matchers with OptionValues with Http4sDsl[IO] with Http4sClientDsl[IO] {

  test("Get single document") {
    val binder = StubBinder("testBinder")
    val bson = new BsonObject()
    bson.put("hello", "world")
    binder.put("testDocument", bson)

    val restService: java.util.function.Function[Request[IO], IO[RestServiceAction[_]]] = {
      case GET -> Root / "binders" / binderName / documentId =>
        IO.pure(new RestServiceAction.RetrieveSingleDocument(binder.binderStore, binderName, documentId))
    }

    val httpService: HttpService[IO] = new Http4sRestAdapter[IO].adapt(restService)
    val request = GET(uri("/binders/testBinder/testDocument")).unsafeRunSync()
    val response = httpService.orNotFound(request).unsafeRunSync()

    response.status.isSuccess shouldBe true
    val responseEntity = new BsonObject()
    responseEntity.put("hello", "world")
    response.as[BsonObject].unsafeRunSync() shouldBe responseEntity
  }

  test("Execute command") {
    val commandManager = new StubCommandManager()

    val restService: java.util.function.Function[Request[IO], IO[RestServiceAction[_]]] = {
      case req @ POST -> Root / "commands" / "helloWorld" =>
        req.as[BsonObject].map { context =>
          new RestServiceAction.ExecuteCommand(commandManager, "helloWorld", context)
        }
    }
    val httpService: HttpService[IO] = new Http4sRestAdapter[IO].adapt(restService)

    val requestContext = new BsonObject()
    requestContext.put("hello", "world")

    val request = POST(uri("/commands/helloWorld"), requestContext).unsafeRunSync()
    val response = httpService.orNotFound(request).unsafeRunSync()

    response.status.isSuccess shouldBe true

    commandManager.executed should have size(1)
    val (commandName, context) = commandManager.executed.head

    commandName shouldBe "helloWorld"
    context.getString("hello") shouldBe "world"
  }

  test("List document ids") {
    val binder = StubBinder("testBinder")

    val bson = new BsonObject()
    bson.put("hello", "world")
    binder.put("doc1", bson)
    binder.put("doc2", bson)

    val restService: java.util.function.Function[Request[IO], IO[RestServiceAction[_]]] = {
      case GET -> Root / "binders" / binderName =>
        IO.pure(new RestServiceAction.ListDocumentIds(binder.binderStore, binderName))
    }

    val httpService: HttpService[IO] = new Http4sRestAdapter[IO].adapt(restService)
    val request = GET(uri("/binders/testBinder")).unsafeRunSync()
    val response = httpService.orNotFound(request).unsafeRunSync()

    response.status.isSuccess shouldBe true
    val actualResponse = response.as[BsonArray].unsafeRunSync()

    actualResponse.size() shouldBe 2
    actualResponse.contains("doc1") shouldBe true
    actualResponse.contains("doc2") shouldBe true
  }


  test("List binders") {
    val binder = StubBinder("testBinder")

    val restService: java.util.function.Function[Request[IO], IO[RestServiceAction[_]]] = {
      case GET -> Root / "binders" =>
        IO.pure(new RestServiceAction.ListBinders(binder.binderStore))
    }

    val httpService: HttpService[IO] = new Http4sRestAdapter[IO].adapt(restService)
    val request = GET(uri("/binders")).unsafeRunSync()
    val response = httpService.orNotFound(request).unsafeRunSync()

    response.status.isSuccess shouldBe true
    val actualResponse = response.as[BsonArray].unsafeRunSync()

    actualResponse.size() shouldBe 1
    actualResponse.contains("testBinder") shouldBe true
  }

  test("Execute query") {
    val bson = new BsonObject()
    bson.put("hello", "world")

    val stubQueryResult =
      Map(
        "doc1" -> bson,
        "doc2" -> bson
      )

    val query = StubQuery("testQuery", stubQueryResult)
    val queryManager = query.stubQueryManager()

    val restService: java.util.function.Function[Request[IO], IO[RestServiceAction[_]]] = {
      case req @ POST -> Root / "query" / queryName =>
        req.as[BsonObject].map { context =>
          new RestServiceAction.RunQuery(queryManager, queryName, context)
        }
    }

    val httpService: HttpService[IO] = new Http4sRestAdapter[IO].adapt(restService)
    val request = POST(uri("/query/testQuery"), bson).unsafeRunSync()
    val response = httpService.orNotFound(request).unsafeRunSync()

    response.status.code shouldBe 200
    val actualResponse = response.as[BsonObject].unsafeRunSync()

    query.executedContexts should have size 1
    query.executedContexts.head shouldBe bson

    actualResponse.size() shouldBe 2
    actualResponse.getBsonObject("doc1") shouldBe bson
    actualResponse.getBsonObject("doc2") shouldBe bson
  }

}
