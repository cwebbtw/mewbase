package example.gettingstarted.commandrest

import cats.effect.{Effect, IO}
import com.typesafe.config.ConfigFactory
import fs2.StreamApp
import io.circe.Json
import io.mewbase.bson.BsonObject
import io.mewbase.cqrs.CommandManager
import io.mewbase.eventsource.EventSink
import io.mewbase.rest.{RestServiceAction, RestServiceAdaptor}
import io.mewbase.rest.http4s.Http4sRestServiceActionVisitor
import org.http4s.HttpService
import org.http4s.dsl.Http4sDsl
import org.http4s.circe._
import org.http4s.server.blaze.BlazeBuilder
import io.mewbase.rest.http4s.BsonEntityCodec._

import scala.concurrent.ExecutionContext.Implicits.global

/*
This example shows how to use a expose a mewbase component (CommandManager) through an
existing http4s service
 */

class HelloWorldService(commandManager: CommandManager) extends Http4sDsl[IO] {

  val actionVisitor = new Http4sRestServiceActionVisitor[IO]

  val helloWorldService: HttpService[IO] = HttpService[IO] {

    case GET -> Root / "hello" / name =>
      Ok(Json.obj("message" -> Json.fromString(s"hello $name")))

    case req@POST -> Root / "buy" =>
      req.as[BsonObject].flatMap { body =>
        actionVisitor.visit(RestServiceAction.executeCommand(commandManager, "buy", body))
      }

  }

}

object Main extends StreamApp[IO] {
  val config = ConfigFactory.load("example.gettingstarted.commandrest/configuration.conf")

  val eventSink = EventSink.instance(config)
  val commandManager = CommandManager.instance(eventSink)

  val buyCommand =
    commandManager
        .commandBuilder()
        .named("buy")
        .emittingTo("purchase_events")
        .as { params =>
          val event = new BsonObject
          event.put("product", params.getString("product")) // product copied from incoming HTTP post
          event.put("quantity", params.getInteger("quantity")) // quantity copied from incoming HTTP post
          event.put("action", "BUY") // action is always BUY
          event
        }
        .create()


  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, StreamApp.ExitCode] =
    BlazeBuilder[IO]
        .bindHttp(8080, "0.0.0.0")
        .mountService(new HelloWorldService(commandManager).helloWorldService, "/")
        .serve
}
