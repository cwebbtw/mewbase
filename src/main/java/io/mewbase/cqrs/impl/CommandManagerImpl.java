package io.mewbase.cqrs.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Command;
import io.mewbase.cqrs.CommandBuilder;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.eventsource.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
        return null;
    }

    @Override
    public Stream<Command> getCommands() {
        return commands.values().stream();
    }

    @Override
    public CompletableFuture<BsonObject> execute(String commandName, BsonObject context) {
        return null;
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
