package io.mewbase.cqrs;

import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 10/01/17.
 */
public interface QueryManager  {

    public BsonObject queryBuilder();

    public CompletableFuture<BsonObject> execute(BsonObject context);

}
