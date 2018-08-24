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

  /*
  Used to extract the value of the `quantity` field
  of a BsonObject as a BigDecimal
   */
  private val quantityLens = (Prism.id[BsonObject]
    composeOptional index("quantity")
    composePrism bsonNumberPrism)

  /*
  Used to extract the value of the `product` field
  of a BsonObject as a String
 */
  private val productLens = (Prism.id[BsonObject]
    composeOptional index("product")
    composePrism bsonStringPrism)

  /*
  A command produces an event based on a set of parameters
  The event is a `BsonObject` and the presence of the parameters is validated
   */
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

  /*
  A command manager is associated with a sink event sink (somewhere event can be written to)
  Each command registered with the command manager routes events to a specified channel on the
  event sink (in this case, `purchase_events`).
   */
  val commands: CommandManager =
    commandManager(eventSink, buy -> "purchase_events", refund -> "purchase_events")

  /*
  A standard Http4s service that exposes 2 POST endpoints: buy and refund
  Mewbase is used to:
  decode the (JSON) post body into BSON
  execute commands with the POST body passed as parameters
  this results in events being emitted to the event sink
   */
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
