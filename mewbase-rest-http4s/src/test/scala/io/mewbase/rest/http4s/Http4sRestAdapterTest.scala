package io.mewbase.rest.http4s

import cats.effect.IO
import io.mewbase.binders.BinderStore
import io.mewbase.bson.BsonObject
import io.mewbase.rest.RestServiceAction
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpService, Method, Request, Uri}
import org.scalatest.{FunSuite, Matchers, OptionValues}
import org.http4s.implicits._
import org.http4s.client.dsl.Http4sClientDsl
import BsonObjectEntityCodec._

class Http4sRestAdapterTest extends FunSuite with Matchers with OptionValues with Http4sDsl[IO] with Http4sClientDsl[IO] {

  test("Get single document") {
    val binder = StubBinder("testBinder")
    val bson = new BsonObject()
    bson.put("hello", "world")
    binder.put("testDocument", bson)

    val restService: java.util.function.Function[Request[IO], IO[RestServiceAction]] = {
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

    val restService: java.util.function.Function[Request[IO], IO[RestServiceAction]] = {
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

}
