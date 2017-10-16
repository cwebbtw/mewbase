package io.mewbase.cqrs.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Query;
import io.mewbase.cqrs.QueryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

/**
 * Created by tim on 10/01/17.
 */
public class QueryBuilderImpl implements QueryBuilder {

    private final static Logger logger = LoggerFactory.getLogger(QueryBuilderImpl.class);

    private final QueryManagerImpl queryManager;

    private  QueryImpl query;

    public QueryBuilderImpl(QueryManagerImpl queryManager) {
        this.queryManager = queryManager;

    }

    @Override
    public QueryBuilder from(String binderName) {
        query.setBinderName(binderName);
        return this;
    }

    @Override
    public QueryBuilder documentFilter(BiFunction<BsonObject, BsonObject, Boolean> documentFilter) {
        query.setDocumentFilter(documentFilter);
        return this;
    }

    @Override
    public Query create() {
        if (query.getBinderName() == null) {
            throw new IllegalStateException("Please specify a binder name");
        }
        if (query.getDocumentFilter() == null && query.getIdSelector() == null) {
            throw new IllegalStateException("Please specify either a document filter or id selector");
        }
        if (query.getDocumentFilter() != null && query.getIdSelector() != null) {
            throw new IllegalStateException("Can't set both document filter and id selector");
        }
        try {
            queryManager.registerQuery(query);
        } catch (Exception e) {
            logger.error("Failed to register query");
        }
        return query;
    }
}
