package io.mewbase.rest.http4s

import cats.effect.IO
import io.circe.Json
import io.mewbase.binders.BinderStore
import io.mewbase.bson.BsonObject
import io.mewbase.rest.RestServiceAction
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpService, Request, Uri}
import org.scalatest.{FunSuite, Matchers, OptionValues}
import org.http4s.implicits._
import org.http4s.circe._


class Http4sRestAdapterTest extends FunSuite with Matchers with OptionValues with Http4sDsl[IO] {

  def testBinderStore(): BinderStore = {
    val binder = StubBinder("testBinder")
    val bson = new BsonObject()
    bson.put("hello", "world")
    binder.put("testDocument", bson)
    binder.binderStore
  }


  test("Get document: only binder store configured") {
    val binderStore = testBinderStore()

    val restService: java.util.function.Function[Request[IO], RestServiceAction] = {
      case GET -> Root / "binders" / binderName / documentId =>
        new RestServiceAction.RetrieveSingleDocument(binderStore, binderName, documentId)
    }

    val httpService: HttpService[IO] = new Http4sRestAdapter[IO].adapt(restService)
    val request = Request[IO](uri=Uri.unsafeFromString("/binders/testBinder/testDocument"))
    val response = httpService.orNotFound(request).unsafeRunSync()

    response.status.isSuccess shouldBe true
    response.as[Json].unsafeRunSync() shouldBe Json.obj("hello" -> Json.fromString("world"))
  }

}
