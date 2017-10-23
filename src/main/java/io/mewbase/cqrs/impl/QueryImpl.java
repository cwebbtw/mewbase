package io.mewbase.cqrs.impl;


import io.mewbase.binders.Binder;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Query;


import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * Created by tim on 10/01/17.
 */
public class QueryImpl implements Query {

    // Inner immutable class for getting results back.
    class ResultImpl implements Result {

        final String id;
        final BsonObject document;

        ResultImpl(String id, BsonObject document) {
            this.id = id;
            this.document = document;
        }

        @Override
        public String getId() { return id; }

        @Override
        public BsonObject getDocument() { return document; }
    }

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
    public Stream<Result> execute(BsonObject params) {
        if ( documentFilter != null) {
            return resultStream( binder.getIdsWithFilter(documentFilter).join() );
        } else {
            // run the id selector on the params
            return resultStream( Stream.of(idSelector.apply(params)));
        }
    }

    private Stream<Result> resultStream( Stream<String> ids) {
        return ids.map(id -> {
            BsonObject doc = binder.get(id).join();
            return new ResultImpl(id, doc);
        });
    }


}
