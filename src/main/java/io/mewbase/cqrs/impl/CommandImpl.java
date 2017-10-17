package io.mewbase.cqrs.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Command;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


/**
 * Created by Nige on 16/10/17.
 */
public class CommandImpl implements Command {

    private final String name;
    private final String outputChannelName;
    private final Function <BsonObject, BsonObject> function;

    /**
     * Can only be constructed from a package local CommandBuilder
     * @param name
     * @param outputChannelName
     * @param function
     */
    CommandImpl(String name, String outputChannelName, Function <BsonObject, BsonObject> function ) {
        this.name = name;
        this.outputChannelName = outputChannelName;
        this.function = function;
    }

    @Override
    public Function<BsonObject, BsonObject> getFunction() {
        return function;
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getOutputChannel() { return  outputChannelName; }


    @Override
    public CompletableFuture<BsonObject> execute(BsonObject context) {
        CompletableFuture<BsonObject> fut = new CompletableFuture<>();
        try {
            fut.complete(function.apply(context));
        } catch (Exception exp) {
            fut.completeExceptionally(exp);
        }
        return fut;
    }


}
