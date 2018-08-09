package example.gettingstarted.projectionpostgres

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.typesafe.config.ConfigFactory
import io.mewbase.binders.BinderStore
import io.mewbase.bson.{BsonObject, BsonPrisms, BsonValue}
import io.mewbase.eventsource.EventSource
import io.mewbase.projection.ProjectionManager
import io.mewbase.bson.syntax._
import monocle.{POptional, Prism}
import monocle.syntax.apply._

import scala.{BigDecimal => _}
import java.math.BigDecimal

object Main extends App  {

  import Lenses._

  private val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  val resourceBasename = "example/gettingstarted/projectionpostgres/configuration.conf"
  val config = ConfigFactory.load(resourceBasename)

  val eventSource: EventSource = EventSource.instance()
  val binderStore: BinderStore = BinderStore.instance()
  val projectionManager: ProjectionManager = ProjectionManager.instance(eventSource, binderStore)

  projectionManager.builder()
    .projecting("purchase_events")
    .identifiedBy { event =>
      val product =
        productLens.getOption(event.getBson).getOrElse("<unknown>")

      val timestamp = event.getInstant
      val zonedDateTime = ZonedDateTime.ofInstant(timestamp, ZoneOffset.UTC)

      product + "_" + dateFormatter.format(zonedDateTime)
    }
    .filteredBy { event =>
      val action = actionLens.getOption(event.getBson)

      action.contains("BUY") || action.contains("REFUND")
    }
    .onto("sales_summary")
    .onto("sales_summary_projection")
    .as { (document, event) =>
      val buys = buysLens.getOption(document).getOrElse(BigDecimal.ZERO)
      val refunds = refundsLens.getOption(document).getOrElse(BigDecimal.ZERO)

      val action = actionLens.getOption(event.getBson)
      val quantity = quantityLens.getOption(event.getBson).getOrElse(BigDecimal.ZERO)

      val newBuys =
        action match {
          case Some("BUY") => buys.add(quantity)
          case _ => buys
        }

      val newRefunds =
        action match {
          case Some("REFUND") => refunds.add(quantity)
          case _ => refunds
        }

      (document
        applyLens at("buys") set Some(newBuys.bsonValue)
        applyLens at("refunds") set Some(newRefunds.bsonValue)
        applyLens at("total") set Some(newBuys.subtract(newRefunds).bsonValue))
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

  val buysLens: POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("buys")
  val refundsLens: POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("refunds")
  val actionLens: POptional[BsonObject, BsonObject, String, String] =
    stringField("action")
  val productLens: POptional[BsonObject, BsonObject, String, String] =
    stringField("product")
  val quantityLens: POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("quantity")
  val totalLens: POptional[BsonObject, BsonObject, BigDecimal, BigDecimal] =
    numberField("total")
}
