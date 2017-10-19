package io.mewbase.cqrs.impl;

import io.mewbase.binders.Binder;
import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.Query;
import io.mewbase.cqrs.QueryBuilder;


import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by tim on 10/01/17.
 */
public class QueryBuilderImpl implements QueryBuilder {

    private final QueryManagerImpl queryManager;

    private String queryName;
    private String binderName;
    private Predicate<BsonObject> documentFilter = null;
    private Function<BsonObject, String> idSelector = null;

    QueryBuilderImpl(QueryManagerImpl queryManager) {
        this.queryManager = queryManager;
    }

    @Override
    public QueryBuilder named(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public QueryBuilder from(String binderName) {
        this.binderName = binderName;
        return this;
    }

    @Override
    public QueryBuilder filteredBy(Predicate<BsonObject> documentFilter) {
        this.documentFilter = documentFilter;
        return this;
    }

    @Override
    public  QueryBuilder selectedBy(Function<BsonObject, String> idSelector) {
        this.idSelector = idSelector;
        return this;
    }

    @Override
    public Query create() {
        if (queryName == null) {
            throw new IllegalStateException("Please specify a query name");
        }
        if (binderName == null) {
            throw new IllegalStateException("Please specify a binder name");
        }
        if (documentFilter == null && idSelector == null) {
            throw new IllegalStateException("Please specify either a document filter or id selector");
        }
        if (documentFilter != null && idSelector != null) {
            throw new IllegalStateException("Can't set both document filter and id selector");
        }
        Binder binder = queryManager.getStore().get(binderName).get();
        Query query = new QueryImpl(queryName,binder,documentFilter,idSelector);
        queryManager.registerQuery(query);
        return query;
    }
}
