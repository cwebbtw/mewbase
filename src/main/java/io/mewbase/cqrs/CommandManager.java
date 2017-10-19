package io.mewbase.cqrs;


import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.impl.CommandManagerImpl;
import io.mewbase.eventsource.EventSink;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


/**
 * A CommandManager is responsible for providing client code with a CommandBuilder.
 * The CommandBuilder can be used to produce a command that is stored in the CommandHandler.
 *
 * The CommandManager can be queried by 'ServerAdaptors' for example in order to construct REST
 * based versions of the Commands.
 */
public interface CommandManager {

    /**
     * Factory method for CommandManager.
     * Given an EventSink return a new Instance of a CommandManger
     *
     * @param sink - The EventSink on which to emmit command events.
     * @return
     */
    static CommandManager instance(EventSink sink)  {
        return new CommandManagerImpl(sink);
    }




    /**
     * The way to construct commands is to use a fuilent CommandBuilder to build
     * and register the new command with the manager.
     */
    CommandBuilder commandBuilder();

    /**
     * Attempt to get a command given it's name.
     * @param commandName
     * @return a CompletableFuture of command or exception
     */
    CompletableFuture<Command> getCommand(String commandName);

    /**
     * List all of the current commands in the Handler
     * @return A stream of all of the current commands
     */
    Stream<Command> getCommands();

    /**
     * Execute a given command in the given context asynconously.
     * @return A future of the resulting event that was sent on the given
     * EventSink's outputChannel.
     */
    CompletableFuture<BsonObject> execute(String commandName, BsonObject context);

}
