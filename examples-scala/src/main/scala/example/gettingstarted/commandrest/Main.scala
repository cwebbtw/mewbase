package example.gettingstarted.commandrest

import io.mewbase.bson.syntax._
import io.mewbase.binders.BinderStore
import io.mewbase.bson.{BsonObject, BsonPrisms, BsonValue}
import io.mewbase.cqrs.{Command, CommandManager}
import io.mewbase.eventsource.{EventSink, EventSource}
import io.mewbase.projection.ProjectionManager
import io.mewbase.rest._
import monocle.Prism

object Main extends App with BsonPrisms {

  val restServiceAdaptor: RestServiceAdaptor = RestServiceAdaptor.instance()
  val binderStore: BinderStore = BinderStore.instance()
  val eventSink: EventSink = EventSink.instance()
  val commandManager: CommandManager = CommandManager.instance(eventSink)
  val eventSource: EventSource = EventSource.instance()
  val projectionManager: ProjectionManager = ProjectionManager.instance(eventSource, binderStore)
  val projectionOutputChannel = "processed_purchase_events"

  private val quantityLens = (Prism.id[BsonObject]
    composeOptional index("body")
    composePrism bsonObjectPrism
    composeOptional index("quantity")
    composePrism bsonNumberPrism)

  private val productLens = (Prism.id[BsonObject]
    composeOptional index("body")
    composePrism bsonObjectPrism
    composeOptional index("product")
    composePrism bsonStringPrism)

  def productAndQuantity(request: BsonObject): (BsonValue, BsonValue) = {
    val product =
      productLens.getOption(request)
        .fold(BsonValue.nullValue())(BsonValue.of)

    val quantity =
      quantityLens.getOption(request)
        .fold(BsonValue.nullValue())(BsonValue.of)

    (product, quantity)
  }

  val buyCommand: Command =
    commandManager
        .commandBuilder()
        .named("buy")
        .as { params =>
          val (product, quantity) = productAndQuantity(params)

          bsonObject(
            "product" -> product,
            "quantity" -> quantity,
            "action" -> "BUY".bsonValue
          )
        }
        .emittingTo("purchase_events")
        .create()

  val refundCommand: Command =
    commandManager
        .commandBuilder()
        .named("refund")
        .as { params =>
          val (product, quantity) = productAndQuantity(params)

          bsonObject(
            "product" -> product,
            "quantity" -> quantity,
            "action" -> "REFUND".bsonValue
          )
        }
        .emittingTo("purchase_events")
        .create()

  restServiceAdaptor.exposeCommand(commandManager, buyCommand.getName)
  restServiceAdaptor.exposeCommand(commandManager, refundCommand.getName)

  restServiceAdaptor.start()
}
