package io.mewbase.cqrs;

import io.mewbase.bson.BsonObject;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Created by tim on 07/01/17.
 */
public interface CommandBuilder {

    CommandBuilder named(String name);

    CommandBuilder as(Function<BsonObject, BsonObject> commandHandler);

    CommandBuilder emittingTo(String channelName);

    Command create();
}
