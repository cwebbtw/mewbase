package io.mewbase.cqrs.impl;


import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Query;


import java.util.HashSet;
import java.util.Set;
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
    private final Function<BsonObject, Set<String>> idSelector;


    QueryImpl(String name,
              Binder binder,
              Function<BsonObject, Set<String>> idSelector,
              Predicate<BsonObject> documentFilter
              ) {
        this.name = name;
        this.binder = binder;
        this.idSelector = idSelector;
        this.documentFilter = documentFilter;

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
    public Function<BsonObject, Set<String>> getIdSelector() {
        return idSelector;
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> execute(BsonObject params) {
        // Emtpy is match all or apply the
        Set<String> keySet = new HashSet();
        if (idSelector != null) keySet = idSelector.apply(params);
        return  binder.getDocuments(keySet,documentFilter);
    }

}
