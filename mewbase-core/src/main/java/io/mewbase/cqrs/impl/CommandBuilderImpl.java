package io.mewbase.cqrs.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Command;
import io.mewbase.cqrs.CommandBuilder;

import java.util.function.Function;

/**
 * Created by tim on 10/01/17.
 */
public class CommandBuilderImpl implements CommandBuilder {

    // need to attach the build command back to the hander once constructed
    private final CommandManagerImpl commandManager;

    // The Command parameters that can be built fluidly
    private String name;
    private String outputChannelName;
    private Function <BsonObject, BsonObject> function;


    CommandBuilderImpl(CommandManagerImpl commandManager) {
        this.commandManager = commandManager;
    }

    @Override
    public CommandBuilder named(String name) {
        this.name = name;
        return this;
    }

    @Override
    public CommandBuilder as(Function<BsonObject, BsonObject> function) {
        this.function = function;
        return this;
    }

    @Override
    public CommandBuilder emittingTo(String outputChannelName) {
        this.outputChannelName = outputChannelName;
        return this;
    }

    @Override
    public Command create() {

        if (this.name == null) {
            throw new IllegalStateException("Please specify a command name");
        }
        if (this.outputChannelName == null) {
            throw new IllegalStateException("Please specify an output channel name");
        }
        if (this.function == null) {
            throw new IllegalStateException("Please specify an input function");
        }
        Command command = new CommandImpl(name,outputChannelName,function);
        commandManager.registerCommand(command);
        return command;
    }
}
