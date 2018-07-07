package io.mewbase.rest

import java.util.concurrent.CountDownLatch

import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import io.circe.{Json, parser}
import io.mewbase.bson.BsonObject
import io.mewbase.rest.http4s.Http4sRestServiceAdapter
import io.mewbase.rest.vertx.VertxRestServiceAdapter
import io.vertx.core.{AsyncResult, Vertx}
import org.http4s.{MediaType, Uri}
import org.http4s.client.Client
import org.http4s.client.blaze.Http1Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.http4s.circe._
import org.http4s.headers.`Content-Type`
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers, OptionValues}

class VertxRestServiceAdaterTest extends RestServiceAdapterTest {
  override def withAdapter[T](adapterToResult: RestServiceAdaptor => T): T = {
    val vertx = Vertx.vertx()
    val config = ConfigFactory.load()
    config.withValue("mewbase.api.rest.vertx.port", ConfigValueFactory.fromAnyRef(Integer.valueOf(0)))

    val adapter = new VertxRestServiceAdapter(config)
    val result = adapterToResult(adapter)

    result
  }
}


class Http4sRestServiceAdapterTest extends RestServiceAdapterTest {
  override def withAdapter[T](adapterToResult: RestServiceAdaptor => T): T = {
    val config = ConfigFactory.load()
    val configWithPort = config.withValue("mewbase.api.rest.http4s.port", ConfigValueFactory.fromAnyRef(Integer.valueOf(0)))

    val adapter = new Http4sRestServiceAdapter(configWithPort)
    val result = adapterToResult(adapter)

    result
  }
}

trait RestServiceAdapterTest extends FunSuite with Http4sDsl[IO] with Http4sClientDsl[IO] with Matchers with BeforeAndAfterAll with OptionValues {

  val binder = StubBinder("testBinder")
  val binderStore = binder.binderStore

  val bson = new BsonObject()
  bson.put("hello", "world")
  binder.put("testDocument", bson)
  binder.put("testDocument2", bson)

  val stubQueryResult =
    Map(
      "doc1" -> bson,
      "doc2" -> bson
    )

  def withAdapter[T](adapterToResult: RestServiceAdaptor => T): T

  val client: Client[IO] = Http1Client[IO]().unsafeRunSync()

  def withRunningServer[T](adapter: RestServiceAdaptor)(logic: => T): T = {
    adapter.start().get()
    var result: Option[T] = None
    try {
      result = Some(logic)
    }
    finally {
      adapter.stop().get()
    }

    result.get
  }

  test("exposeGetDocument(BinderStore) list binders") {
    withAdapter { adapter =>
      adapter.exposeGetDocument(binderStore)

      withRunningServer(adapter) {
        val json = client.expect[Json](s"http://localhost:${adapter.getServerPort}/binders").unsafeRunSync()
        val jsonArray = json.asArray.value
        jsonArray shouldBe Vector(Json.fromString("testBinder"))
      }
    }
  }

  test("exposeGetDocument(BinderStore) list document ids") {
    withAdapter { adapter =>
      adapter.exposeGetDocument(binderStore)

      withRunningServer(adapter) {
        val json = client.expect[Json](s"http://localhost:${adapter.getServerPort}/binders/testBinder").unsafeRunSync()
        val jsonArray = json.asArray.value
        jsonArray shouldBe Vector(Json.fromString("testDocument"), Json.fromString("testDocument2"))
      }
    }
  }

  test("exposeGetDocument(BinderStore) retrieve document") {
    withAdapter { adapter =>
      adapter.exposeGetDocument(binderStore)

      withRunningServer(adapter) {
        val json = client.expect[Json](s"http://localhost:${adapter.getServerPort}/binders/testBinder/testDocument").unsafeRunSync()
        val jsonObject = json.asObject.value
        jsonObject.keys should have size 1
        jsonObject("hello").value shouldBe Json.fromString("world")
      }
    }
  }

  test("exposeGetDocument(BinderStore, uriPathPrefix) list binders") {
    withAdapter { adapter =>
      adapter.exposeGetDocument(binderStore, "/foo")

      withRunningServer(adapter) {
        val json = client.expect[Json](s"http://localhost:${adapter.getServerPort}/foo").unsafeRunSync()
        val jsonArray = json.asArray.value
        jsonArray shouldBe Vector(Json.fromString("testBinder"))
      }
    }
  }

  test("exposeGetDocument(BinderStore, binderName, uriPathPrefix) list document ids") {
    withAdapter { adapter =>
      adapter.exposeGetDocument(binderStore, "testBinder", "/foo")

      withRunningServer(adapter) {
        val json = client.expect[Json](s"http://localhost:${adapter.getServerPort}/foo/testBinder").unsafeRunSync()
        val jsonArray = json.asArray.value
        jsonArray shouldBe Vector(Json.fromString("testDocument"), Json.fromString("testDocument2"))
      }
    }
  }

  test("exposeGetDocument(BinderStore, binderName, uriPathPrefix) retrieve document") {
    withAdapter { adapter =>
      adapter.exposeGetDocument(binderStore, "testBinder", "/foo")

      withRunningServer(adapter) {
        val json = client.expect[Json](s"http://localhost:${adapter.getServerPort}/foo/testBinder/testDocument").unsafeRunSync()
        val jsonObject = json.asObject.value
        jsonObject.keys should have size 1
        jsonObject("hello").value shouldBe Json.fromString("world")
      }
    }
  }

  test("exposeCommand(CommandManager, commandName) should execute command") {
    withAdapter { adapter =>
      val commandManager = new StubCommandManager()
      adapter.exposeCommand(commandManager, "helloWorld")

      withRunningServer(adapter) {
        val requestContext = Json.obj("hello" -> Json.fromString("world"))
        val request = POST(
          Uri.unsafeFromString(s"http://localhost:${adapter.getServerPort}/helloWorld"),
          requestContext,
          `Content-Type`(MediaType.`application/json`)).unsafeRunSync()

        client.status(request).unsafeRunSync().code shouldBe 200

        commandManager.executed should have size (1)
        val (commandName, context) = commandManager.executed.head

        commandName shouldBe "helloWorld"
        context.getString("hello") shouldBe "world"
      }
    }
  }

  test("exposeCommand(CommandManager, commandName, uriPathPrefix) should execute command") {
    withAdapter { adapter =>
      val commandManager = new StubCommandManager()
      adapter.exposeCommand(commandManager, "helloWorld", "/foo")

      withRunningServer(adapter) {
        val requestContext = Json.obj("hello" -> Json.fromString("world"))
        val request = POST(
          Uri.unsafeFromString(s"http://localhost:${adapter.getServerPort}/foo/helloWorld"),
          requestContext,
          `Content-Type`(MediaType.`application/json`)).unsafeRunSync()

        client.status(request).unsafeRunSync().code shouldBe 200

        commandManager.executed should have size (1)
        val (commandName, context) = commandManager.executed.head

        commandName shouldBe "helloWorld"
        context.getString("hello") shouldBe "world"
      }
    }
  }

  test("exposeQuery(QueryManager, queryName)") {
    withAdapter { adapter =>
      val query = StubQuery("testQuery", stubQueryResult)
      val queryManager = query.stubQueryManager()
      adapter.exposeQuery(queryManager, "testQuery")

      withRunningServer(adapter) {
        val request = GET(Uri.unsafeFromString(s"http://localhost:${adapter.getServerPort}/testQuery")).unsafeRunSync()

        val json = client.expect[Json](request).unsafeRunSync()

        query.executedContexts should have size 1
        query.executedContexts.head shouldBe new BsonObject()

        val jsonObject = json.asObject.value
        jsonObject.size shouldBe 2
        val bsonAsJson = parser.parse(bson.encodeToString()).toOption.value
        jsonObject("doc1").value shouldBe bsonAsJson
        jsonObject("doc2").value shouldBe bsonAsJson
      }
    }
  }

}
