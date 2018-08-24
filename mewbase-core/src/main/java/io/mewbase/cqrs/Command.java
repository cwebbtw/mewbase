package io.mewbase.cqrs;

import io.mewbase.bson.BsonObject;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A Command is in essence a named function that maps a command name and some input context (paramters) parameters in the
 * form of a BsonObject
 */
public interface Command {

    String getName();

    String getOutputChannel();

    Function<BsonObject, BsonObject> getFunction();

    CompletableFuture<BsonObject> execute(BsonObject context);

}
