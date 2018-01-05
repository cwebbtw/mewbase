package io.mewbase.cqrs.impl;


import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Query;

import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * Created by tim on 10/01/17.
 */
public class QueryImpl implements Query {

    private final String name;
    private final Binder binder;
    private final BiPredicate<BsonObject,KeyVal<String, BsonObject>> queryFilter;

    QueryImpl(String name,
              Binder binder,
              BiPredicate<BsonObject, KeyVal<String, BsonObject>> queryFilter) {
        this.name = name;
        this.binder = binder;
        this.queryFilter = queryFilter;
    }

    @Override
    public String getName() { return name; }

    @Override
    public Binder getBinder() {
        return binder;
    }

    @Override
    public BiPredicate<BsonObject, KeyVal<String, BsonObject>> getQueryFilter() {
        return queryFilter;
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> execute(BsonObject context) {
        Predicate<KeyVal<String, BsonObject>> docFilter = (kv) -> queryFilter.test(context,kv);
        return binder.getDocuments(docFilter);
    }

}
