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

object Main extends App {

  import Lenses._

  val resourceBasename = "example/gettingstarted/projectionpostgres/configuration.conf"
  val config           = ConfigFactory.load(resourceBasename)

  val eventSource: EventSource = EventSource.instance()
  val binderStore: BinderStore = BinderStore.instance()

  val salesSummary =
    projection("sales_summary")
      .classify { event =>
        for {
          product <- productLens.getOption(event.getBson)
          action <- actionLens.getOption(event.getBson) if Set("BUY", "REFUND").contains(action)
        }
          yield s"${product}_${utcDate(event.getInstant)}"
      }
      .project { (document, event) =>
        val buys = buysLens.getOption(document).getOrElse(BigDecimal.ZERO)
        val refunds = refundsLens.getOption(document).getOrElse(BigDecimal.ZERO)

        val action = actionLens.getOption(event.getBson)
        val quantity = quantityLens.getOption(event.getBson).getOrElse(BigDecimal.ZERO)

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

        (document
          applyLens at("buys") set Some(newBuys.bsonValue)
          applyLens at("refunds") set Some(newRefunds.bsonValue)
          applyLens at("total") set Some(newBuys.subtract(newRefunds).bsonValue))
      }

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
