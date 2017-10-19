package io.mewbase.cqrs.impl;

import io.mewbase.binders.BinderStore;


import io.mewbase.binders.Binder;

import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.QueryBuilder;
import io.mewbase.cqrs.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;



/**
 * Created by tim on 10/01/17 as CQRSManager
 */
class QueryManagerImpl implements QueryManager {

    private final static Logger logger = LoggerFactory.getLogger(QueryManager.class);

    private final BinderStore store;

    private final Map<String, QueryImpl> queries = new ConcurrentHashMap<>();


    public QueryManagerImpl(BinderStore store) {
        this.store = store;
    }

    /**
     *
     * @param query
     */
    synchronized void registerQuery(QueryImpl query)  {
        if (queries.containsKey(query.getName())) {
            throw new IllegalArgumentException("Query " + query.getName() + " already registered");
        }

        Binder binder = store.open(query.getBinderName());
        if (binder == null) {
            throw new IllegalArgumentException("No such binder " + query.getBinderName());
        }
        query.setBinder(binder);
        queries.put(query.getName(), query);
    }


    public QueryImpl getQuery(String queryName) {
        return queries.get(queryName);
    }

    @Override
    public QueryBuilder queryBuilder() {
        return null;
    }


    @Override
    public CompletableFuture<BsonObject> execute(BsonObject context) {
        return null;
    }

}
