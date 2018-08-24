package example.gettingstarted.commandrest

import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import fs2.StreamApp
import io.mewbase.bson.syntax._
import io.mewbase.cqrs.syntax._
import io.mewbase.bson.{BsonObject, BsonPrisms, BsonValue}
import io.mewbase.cqrs.CommandManager
import io.mewbase.eventsource.EventSink
import io.mewbase.rest._
import io.mewbase.rest.http4s.MewbaseSupport
import monocle.Prism
import org.http4s.HttpService
import org.http4s.dsl.Http4sDsl
import io.mewbase.rest.http4s.BsonEntityCodec._
import org.http4s.server.blaze.BlazeBuilder
import scala.concurrent.ExecutionContext.Implicits.global

/*
This example exposes 2 commands through a Http4s service:
Buy
Refund

These commands emit an event to the configured event sink, which includes
product and quantity attributes from the POST body

Sample request for this service:
curl --verbose -H 'Content-Type: application/json' --data "{\"product\": \"banana\", \"quantity\": 1231}" -X POST http://localhost:9000/buy

 */
object Main extends StreamApp[IO] with Http4sDsl[IO] with BsonPrisms with MewbaseSupport {

  val config: Config = ConfigFactory.load("example.gettingstarted.commandrest/configuration.conf")
  val eventSink: EventSink = EventSink.instance(config)

  private val quantityLens = (Prism.id[BsonObject]
    composeOptional index("quantity")
    composePrism bsonNumberPrism)

  private val productLens = (Prism.id[BsonObject]
    composeOptional index("product")
    composePrism bsonStringPrism)

  private val buy =
    command("buy") { params =>
      for {
        product <- productLens.getOption(params).toRight("Missing product")
        quantity <- quantityLens.getOption(params).toRight("Missing quantity")
      }
      yield
        bsonObject(
          "product" -> product.bsonValue,
          "quantity" -> quantity.bsonValue,
          "action" -> "BUY".bsonValue
        )
    }

  private val refund =
    command("refund") { params =>
      for {
        product <- productLens.getOption(params).toRight("Missing product")
        quantity <- quantityLens.getOption(params).toRight("Missing quantity")
      }
        yield
          bsonObject(
            "product" -> product.bsonValue,
            "quantity" -> quantity.bsonValue,
            "action" -> "REFUND".bsonValue
          )
    }

  val commands: CommandManager =
    commandManager(eventSink, buy -> "purchase_events", refund -> "purchase_events")

  val service: HttpService[IO] = HttpService[IO] {
    case req@POST -> Root / "buy" =>
      req.as[BsonObject].flatMap { params =>
        ExecuteCommand(commands, buy.name, params)
      }
    case req@POST -> Root / "refund" =>
      req.as[BsonObject].flatMap { params =>
        ExecuteCommand(commands, refund.name, params)
      }
  }

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, StreamApp.ExitCode] =
    BlazeBuilder[IO]
      .mountService(service, "/")
      .bindLocal(9000)
      .serve
}
