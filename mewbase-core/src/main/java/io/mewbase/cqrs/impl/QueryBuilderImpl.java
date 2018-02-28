package io.mewbase.cqrs.impl;

import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.binders.impl.lmdb.LmdbBinder;
import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.Query;
import io.mewbase.cqrs.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by tim on 10/01/17.
 */
public class QueryBuilderImpl implements QueryBuilder {

    private final static Logger log = LoggerFactory.getLogger(QueryBuilderImpl.class);

    private final QueryManagerImpl queryManager;

    private String queryName;
    private String binderName;
    private BiPredicate<BsonObject, KeyVal<String,BsonObject>> queryFilter;


    private final BiPredicate<BsonObject, KeyVal<String,BsonObject>> DEFAULT_QUERY_FILTER = (ctx,kv) -> true;


    QueryBuilderImpl(QueryManagerImpl queryManager) {
        this.queryManager = queryManager;
        queryFilter = DEFAULT_QUERY_FILTER;
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
    public QueryBuilder filteredBy(BiPredicate<BsonObject, KeyVal<String,BsonObject>> queryFilter) {
        this.queryFilter = queryFilter;
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

        Binder binder = queryManager.getStore().get(binderName).get();
        Query query = new QueryImpl(queryName,binder,queryFilter);
        queryManager.registerQuery(query);
        return query;
    }
}
