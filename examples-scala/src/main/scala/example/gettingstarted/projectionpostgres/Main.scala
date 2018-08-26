package example.gettingstarted.projectionpostgres

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.typesafe.config.ConfigFactory
import io.mewbase.binders.BinderStore
import io.mewbase.bson.{BsonObject, BsonPrisms}
import io.mewbase.eventsource.EventSource
import io.mewbase.bson.syntax._
import monocle.{POptional, Prism}
import monocle.syntax.apply._
import io.mewbase.projection.syntax._

import scala.{BigDecimal => _}
import java.math.BigDecimal

import example.gettingstarted.Action

object Main extends App {

  import Lenses._

  val resourceBasename = "example/gettingstarted/projectionpostgres/configuration.conf"
  val config           = ConfigFactory.load(resourceBasename)

  val eventSource: EventSource = EventSource.instance()
  val binderStore: BinderStore = BinderStore.instance()

  /*
  Builds a sales summary projection
  A projection listens to a stream of events:
  * filters out unintereting events
  * classifies them into an identifier of an aggregation
  * modifies the aggregation
   */
  val salesSummary =
    projection("sales_summary")
      /*
      returns an Option[String] for each event:
      None =>
        if we are _not_ interested in this event
      Some(identifier) =>
        if we are interested in the event, and it should be applied to the aggregation with the
        provided identifier
       */
      .classify { event =>
        for {
          product <- productLens.getOption(event.getBson).toRight("Could not extract product")
          actionString <- actionLens.getOption(event.getBson).toRight("Could not extract action")
          _ <- Action(actionString)
        }
          yield s"${product}_${utcDate(event.getInstant)}"
      }
      /*
      returns the new value of the aggregation after the event has been applied to it
       */
      .project { (document, event) =>
        /*
        values from aggregation
         */
        val buys = buysLens.getOption(document).getOrElse(BigDecimal.ZERO)
        val refunds = refundsLens.getOption(document).getOrElse(BigDecimal.ZERO)

        /*
        values from incoming events
         */
        val action = actionLens.getOption(event.getBson)
        val quantity = quantityLens.getOption(event.getBson).getOrElse(BigDecimal.ZERO)

      /*
      new values after buy or refund applied
       */
        val newBuys =
          action match {
            case Some("BUY") => buys.add(quantity)
            case _           => buys
          }

        val newRefunds =
          action match {
            case Some("REFUND") => refunds.add(quantity)
            case _              => refunds
          }

      /*
      modify buys / refunds / total field in the aggregation
       */
        Right(document
          applyLens at("buys") set Some(newBuys.bsonValue)
          applyLens at("refunds") set Some(newRefunds.bsonValue)
          applyLens at("total") set Some(newBuys.subtract(newRefunds).bsonValue))
      }

  /*
  the projection manager runs projections over the lifetime of the application
  It is configured for a single source of incoming events, and writes aggregations to a single binder store
  each projection is specified along with the:
    source channel in the event source: `purchase_events` in this case
    destination binder in binder store: `sales_summary_projection` in this case
   */
  projectionManager(eventSource, binderStore, "purchase_events" -> salesSummary -> "sales_summary_projection")

  private def utcDate(timestamp: Instant): String = {
    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE
    val zonedDateTime = ZonedDateTime.ofInstant(timestamp, ZoneOffset.UTC)
    dateFormatter.format(zonedDateTime)
  }
}

object Lenses extends BsonPrisms {
  private def numberField(fieldName: String) =
    (Prism.id[BsonObject]
      composeOptional index(fieldName)
      composePrism bsonNumberPrism)

  private def stringField(fieldName: String) =
    (Prism.id[BsonObject]
      composeOptional index(fieldName)
      composePrism bsonStringPrism)

  val buysLens    : POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("buys")
  val refundsLens : POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("refunds")
  val actionLens  : POptional[BsonObject, BsonObject, String, String]         =
    stringField("action")
  val productLens : POptional[BsonObject, BsonObject, String, String]         =
    stringField("product")
  val quantityLens: POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("quantity")
  val totalLens   : POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("total")
}
