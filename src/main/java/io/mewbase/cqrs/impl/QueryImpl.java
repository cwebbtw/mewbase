package io.mewbase.cqrs.impl;

import io.mewbase.binders.Binder;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Query;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * Created by tim on 10/01/17.
 */
public class QueryImpl implements Query {

    private final String name;
    private final Binder binder;
    private final Predicate<BsonObject> documentFilter;
    private final Function<BsonObject, String> idSelector;


    QueryImpl(String name,
              Binder binder,
              Predicate<BsonObject> documentFilter,
              Function<BsonObject, String> idSelector) {
        this.name = name;
        this.binder = binder;
        this.documentFilter = documentFilter;
        this.idSelector = idSelector;
    }

    @Override
    public String getName() { return name; }

    @Override
    public Binder getBinder() {
        return binder;
    }

    @Override
    public Predicate<BsonObject> getDocumentFilter() {
        return documentFilter;
    }

    @Override
    public Function<BsonObject, String> getIdSelector() {
        return idSelector;
    }

    @Override
    public Stream<CompletableFuture<Query.Result>> execute(BsonObject params) {
        // Todo create the stream
        return null;
    }

}
