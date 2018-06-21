package example.gettingstarted.commandrest

import cats.effect.{Effect, IO}
import com.typesafe.config.ConfigFactory
import fs2.StreamApp
import io.circe.Json
import io.mewbase.bson.BsonObject
import io.mewbase.cqrs.CommandManager
import io.mewbase.eventsource.EventSink
import io.mewbase.rest.RestServiceAdaptor
import org.http4s.HttpService
import org.http4s.dsl.Http4sDsl
import org.http4s.circe._
import org.http4s.server.blaze.BlazeBuilder
import scala.concurrent.ExecutionContext.Implicits.global

class HelloWorldService[F[_] : Effect] extends Http4sDsl[F] {
  val helloWorldService: HttpService[F] = HttpService[F] {
    case GET -> Root / "hello" / name =>
      Ok(Json.obj("message" -> Json.fromString(s"hello $name")))
  }
}

object Main extends StreamApp[IO] {
  val config = ConfigFactory.load("example.gettingstarted.commandrest/configuration.conf")

  val eventSink = EventSink.instance(config)
  val restServiceAdaptor = RestServiceAdaptor.instance(config)
  val commandManager = CommandManager.instance(eventSink)

  val buyCommand =
    commandManager
        .commandBuilder()
        .named("buy")
        .emittingTo("purchase_events")
        .as { params =>
          val event = new BsonObject
          event.put("product", params.getBsonObject("body").getString("product")) // product copied from incoming HTTP post
          event.put("quantity", params.getBsonObject("body").getInteger("quantity")) // quantity copied from incoming HTTP post
          event.put("action", "BUY") // action is always BUY
          event
        }
        .create()

  restServiceAdaptor.exposeCommand(commandManager, buyCommand.getName)

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, StreamApp.ExitCode] =
    BlazeBuilder[IO]
        .bindHttp(8080, "0.0.0.0")
        .mountService(new HelloWorldService[IO].helloWorldService, "/")
        .serve
}
