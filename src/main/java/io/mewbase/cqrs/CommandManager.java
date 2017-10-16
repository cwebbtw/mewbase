package io.mewbase.cqrs;

import io.mewbase.bson.BsonObject;

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
     * The way to construct commands is to use a fuilent CommandBuilder to build
     * and register the new command with the manager.
     */
    CommandBuilder commandBuilder();

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
