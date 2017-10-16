package io.mewbase.cqrs.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Command;
import io.mewbase.cqrs.CommandBuilder;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.eventsource.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Created by tim on 10/01/17.
 */
public class CommandManagerImpl implements CommandManager {

    private final static Logger logger = LoggerFactory.getLogger(CommandManager.class);

    private final EventSink eventSink;

    private final Map<String, Command> commands = new ConcurrentHashMap<>();

    public CommandManagerImpl(EventSink eventSink) {
        this.eventSink = eventSink;
    }


    @Override
    public CommandBuilder commandBuilder() {
        return new CommandBuilderImpl(this);
    }

    @Override
    public Optional<Command> getCommand(String commandName) {
        if (commands.containsKey(commandName)) {
            return Optional.of(commands.get(commandName));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Stream<Command> getCommands() {
        return commands.values().stream();
    }

    @Override
    public CompletableFuture<BsonObject> execute(final String commandName, final BsonObject context) {

        CompletableFuture fut = new CompletableFuture();
        Optional<Command> command = getCommand(commandName);
        // TODO Execution Path
        command.ifPresentOrElse(cmd -> fut.complete(new BsonObject()) ,
                () -> fut.completeExceptionally(new NoSuchElementException("No Command for key "+commandName )));
        return fut;
    }


    /**
     * On completion of a newly built command the CommandBuilder  registers the command with
     * CommandManger
     * @param command
     * @return
     */
    Command registerCommand(Command command) {
        // TODO check for duplicate names.
        commands.put(command.getName(),command);
        return null;
    }


}
