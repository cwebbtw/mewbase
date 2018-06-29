package io.mewbase.rest

import java.lang
import java.util.concurrent.CompletableFuture
import java.util.stream

import io.mewbase.bson.BsonObject
import io.mewbase.cqrs.{Command, CommandBuilder, CommandManager}

import scala.collection.mutable.ListBuffer

class StubCommandManager extends CommandManager {

  val executed = ListBuffer[(String, BsonObject)]()
  /**
    * The way to construct commands is to use a fuilent CommandBuilder to build
    * and register the new command with the manager.
    */
  override def commandBuilder(): CommandBuilder = ???

  /**
    * Attempt to get a command given it's name.
    *
    * @param commandName
    * @return a CompletableFuture of command or exception
    */
  override def getCommand(commandName: String): CompletableFuture[Command] = ???

  /**
    * List all of the current commands in the Handler
    *
    * @return A stream of all of the current commands
    */
  override def getCommands: stream.Stream[Command] = ???

  /**
    * Execute a given command in the given context asynconously.
    *
    * @return A future of the resulting event that was sent on the given
    *         EventSink's outputChannel.
    */
  override def execute(commandName: String, context: BsonObject): CompletableFuture[lang.Long] = {
    executed += commandName -> context
    CompletableFuture.completedFuture(1)
  }
}
