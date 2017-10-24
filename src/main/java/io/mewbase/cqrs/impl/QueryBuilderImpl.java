package io.mewbase.cqrs.impl;

import io.mewbase.binders.Binder;
import io.mewbase.binders.impl.lmdb.LmdbBinder;
import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.Query;
import io.mewbase.cqrs.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Optional;
import java.util.Set;
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

    private final Function<BsonObject, Set<String>>  DEFAULT_ID_SELECTOR = null;
    private final Predicate<BsonObject> DEFAULT_DOC_FILTER = document -> true;

    private Function<BsonObject, Set<String>> idSelector;
    private Predicate<BsonObject> documentFilter;


    QueryBuilderImpl(QueryManagerImpl queryManager) {
        this.queryManager = queryManager;
        documentFilter = DEFAULT_DOC_FILTER;
        idSelector = DEFAULT_ID_SELECTOR;
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
    public  QueryBuilder selectedBy(Function<BsonObject, Set<String>> idSelector) {
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
        if ( idSelector == DEFAULT_ID_SELECTOR && documentFilter == DEFAULT_DOC_FILTER ) {
            log.info("Query created with default id selector and doc filter");
        }

        Binder binder = queryManager.getStore().get(binderName).get();
        Query query = new QueryImpl(queryName,binder,idSelector,documentFilter);
        queryManager.registerQuery(query);
        return query;
    }
}
