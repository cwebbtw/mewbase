package example.gettingstarted.allinone

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import cats.effect.IO
import example.gettingstarted.{Action, Buy, Refund}
import fs2.StreamApp
import io.mewbase.{AsDate, Result}
import io.mewbase.binders.BinderStore
import io.mewbase.bson.{BsonObject, BsonPrisms}
import io.mewbase.cqrs.CommandManager
import io.mewbase.eventsource.{EventSink, EventSource}
import io.mewbase.rest.{ExecuteCommand, RetrieveSingleDocument}
import io.mewbase.rest.http4s.MewbaseSupport
import org.http4s.HttpService
import org.http4s.dsl.Http4sDsl
import org.http4s.server.blaze.BlazeBuilder
import io.mewbase.rest.http4s.BsonEntityCodec._
import io.mewbase.cqrs.syntax._
import io.mewbase.bson.syntax._
import io.mewbase.projection.ProjectionManager
import io.mewbase.projection.syntax._

import scala.concurrent.ExecutionContext.Implicits.global

/*
This sample combines those in `commandrest`, `projectionrest` and `projectionpostgres`

It gives a Http4s application exposing 3 endpoints:
POST -> Root / "buy"                                                - raises a BUY event
POST -> Root / "refund"                                             - raises a REFUND event
GET -> Root / "product" / name / "date" / AsDate(date) / "summary"  - gets a summary of buys / refunds per product per day

Example requests:

$ curl --verbose 'Content-Type: application/json' --data "{\"product\": \"banana\", \"quantity\": 1231}" -X POST http://localhost:8080/buy
Sample response:
< HTTP/1.1 200 OK
< Content-Type: text/plain; charset=UTF-8
< Date: Sun, 26 Aug 2018 17:32:53 GMT
< Content-Length: 0

$ curl --verbose http://localhost:8080/product/banana/date/2018-08-26/summary
Sample response:
< HTTP/1.1 200 OK
< Content-Type: application/json
< Date: Sun, 26 Aug 2018 17:32:56 GMT
< Content-Length: 41

{"buys":4924,"refunds":1231,"total":3693}
 */

/*
Expose Mewbase components through a Http4s HttpService
 */
case class Service(
  binderStore: BinderStore,
  commandManager: CommandManager,
  buyCommand: CommandNameAndProcessor,
  refundCommand: CommandNameAndProcessor
) extends Http4sDsl[IO] with MewbaseSupport {

  val service: HttpService[IO] = HttpService[IO] {
    case req@POST -> Root / "buy"                                           =>
      req.as[BsonObject].flatMap { body =>
        ExecuteCommand(commandManager, buyCommand.name, body)
      }
    case req@POST -> Root / "refund"                                        =>
      req.as[BsonObject].flatMap { body =>
        ExecuteCommand(commandManager, refundCommand.name, body)
      }
    case GET -> Root / "product" / name / "date" / AsDate(date) / "summary" =>
      RetrieveSingleDocument(binderStore, Main.salesSummaryBinder, Main.salesSummaryDocumentKey(name, date))
  }

}

/*
Maintains the sales_summary projection based upon incoming purchase events
 */
object Projection {

  val salesSummary: ProjectionNameClassifierAndFunction =
    projection("sales_summary")
      .classify { event =>
        /*
        filter events and compute an aggregate key
         */
        val bson = event.getBson

        for {
          product <- bson.at("product").flatMap(_.to[String])
          action <- bson.at("action").flatMap(_.to[String])
          _ <- Action(action)
          date = LocalDateTime.ofInstant(event.getInstant, ZoneOffset.UTC).toLocalDate
        }
          yield Main.salesSummaryDocumentKey(product, date)
      }
      .project { (document, event) =>
        /*
        alter current document based on incoming event
         */
        val currentBuys =
          document.at("buys").flatMap(_.to[Int]).getOrElse(0)

        val currentRefunds =
          document.at("refunds").flatMap(_.to[Int]).getOrElse(0)

        val bson = event.getBson
        val action = bson.at("action").flatMap(_.to[String]).flatMap(Action.apply)
        val quantity = bson.at("quantity").flatMap(_.to[Int])

        def addQuantityOrGetCurrent(a: Action, current: Int): Result[Int] =
          if (action.contains(a))
            quantity.map(_ + current)
          else
            Right(current)

        val newBuys = addQuantityOrGetCurrent(Buy, currentBuys)
        val newRefunds = addQuantityOrGetCurrent(Refund, currentRefunds)

        for {
          buys <- newBuys
          refunds <- newRefunds
        }
          yield bsonObject(
            "buys" -> buys.bsonValue,
            "refunds" -> refunds.bsonValue,
            "total" -> (buys - refunds).bsonValue)
      }
}

object Commands {

  /*
  Raise purchase events based on data in HTTP POST body
   */

  def buildCommand(action: Action)(params: BsonObject): Result[BsonObject] =
    for {
      product <- params.at("product").flatMap(_.to[String])
      quantity <- params.at("quantity").flatMap(_.to[Int])
    }
      yield bsonObject(
        "product" -> product.bsonValue,
        "quantity" -> quantity.bsonValue,
        "action" -> action.bsonValue)

  val buyCommand: CommandNameAndProcessor =
    command("buy")(buildCommand(Buy))

  val refundCommand: CommandNameAndProcessor =
    command("refund")(buildCommand(Refund))

}

object Main extends StreamApp[IO] with BsonPrisms {

  /*
  Instantiate mewbase components, and start Http4s service
   */

  import Commands._
  import Projection._

  val eventChannel      : String      = "purchase_event"
  val salesSummaryBinder: String      = "sales_summary"
  val binderStore       : BinderStore = BinderStore.instance()
  val source            : EventSource = EventSource.instance()
  val sink              : EventSink   = EventSink.instance()

  def salesSummaryDocumentKey(product: String, date: LocalDate): String =
    s"${product}_${date.format(DateTimeFormatter.ISO_DATE)}"

  val commands: CommandManager =
    commandManager(sink, buyCommand -> eventChannel, refundCommand -> eventChannel)

  val projections: ProjectionManager =
    projectionManager(source, binderStore, eventChannel -> salesSummary -> salesSummaryBinder)

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, StreamApp.ExitCode] =
    BlazeBuilder[IO]
      .mountService(Service(binderStore, commands, buyCommand, refundCommand).service, "/")
      .bindLocal(8080)
      .serve

}