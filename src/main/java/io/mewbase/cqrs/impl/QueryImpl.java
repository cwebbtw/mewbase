package io.mewbase.cqrs.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.binders.Binder;
import io.mewbase.cqrs.Query;


import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * Created by tim on 10/01/17.
 */
public class QueryImpl implements Query {

    private final String name;
    private String binderName;
    private Binder binder;

    private BiFunction<BsonObject, BsonObject, Boolean> documentFilter;

    private Function<BsonObject, String> idSelector;

    public QueryImpl(String name) {
        this.name = name;
    }

    public String getBinderName() {
        return binderName;
    }

    public void setBinderName(String binderName) {
        this.binderName = binderName;
    }

    public BiFunction<BsonObject, BsonObject, Boolean> getDocumentFilter() {
        return documentFilter;
    }

    public void setDocumentFilter(BiFunction<BsonObject, BsonObject, Boolean> documentFilter) {
        this.documentFilter = documentFilter;
    }

    public String getName() {
        return name;
    }

    public Function<BsonObject, String> getIdSelector() {
        return idSelector;
    }

    public void setIdSelector(Function<BsonObject, String> idSelector) {
        this.idSelector = idSelector;
    }

    public Binder getBinder() {
        return binder;
    }

    public void setBinder(Binder binder) {
        this.binder = binder;
    }

}
