package io.mewbase.cqrs

import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.EventSink

package object syntax {

  type Result[T] = Either[String, T]

  case class CommandNameAndProcessor(name: String, processor: BsonObject => Result[BsonObject])
  case class CommandName(name: String) {
    def apply(processor: BsonObject => Result[BsonObject]): CommandNameAndProcessor =
      CommandNameAndProcessor(name, processor)
  }

  def command(name: String): CommandName =
    CommandName(name)

  def commandManager(eventSink: EventSink, commands: (CommandNameAndProcessor, String) *): CommandManager = {
    val result = CommandManager.instance(eventSink)

    commands.foreach {
      case (CommandNameAndProcessor(name, processor), channel) =>
        result
          .commandBuilder()
            .named(name)
            .as(params => {
              val result = processor(params)

              result match {
                case Right(value) => value
                case Left(message) =>
                  throw new IllegalStateException(s"Failed to process command '$name' with parameters '$params': $message")
              }
            })
            .emittingTo(channel)
            .create()
        ()
    }

    result
  }

}
