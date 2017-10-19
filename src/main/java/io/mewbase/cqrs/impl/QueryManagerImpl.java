package io.mewbase.cqrs.impl;

import io.mewbase.binders.BinderStore;


import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.Query;
import io.mewbase.cqrs.QueryBuilder;
import io.mewbase.cqrs.QueryManager;


import java.util.Map;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;


/**
 * Created by tim on 10/01/17 as CQRSManager
 */
public class QueryManagerImpl implements QueryManager {

    private final BinderStore store;

    private final Map<String, Query> queries = new ConcurrentHashMap<>();

    public QueryManagerImpl(BinderStore store) {
        this.store = store;
    }

    synchronized void registerQuery(Query query)  {
        if (queries.containsKey(query.getName())) {
            throw new IllegalArgumentException("Query " + query.getName() + " already registered");
        }
        queries.put(query.getName(), query);
    }

    BinderStore getStore() { return store; }

    @Override
    public QueryBuilder queryBuilder() {
        return new QueryBuilderImpl(this);
    }

    @Override
    public CompletableFuture<Query> getQuery(String queryName)  {
        CompletableFuture fut = new CompletableFuture();
        if (queries.containsKey(queryName)) {
            fut.complete(queries.get(queryName));
        } else {
            fut.completeExceptionally(new NoSuchElementException("No query matching " + queryName));
        }
        return fut;
    }

    @Override
    public Stream<Query> getQueries() {
        return queries.values().stream();
    }

    @Override
    public CompletableFuture<BsonObject> execute(String queryName, BsonObject context) {
        // TODO with Streams
        return null;
    }

}
