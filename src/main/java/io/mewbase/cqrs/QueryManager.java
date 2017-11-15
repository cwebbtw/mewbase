package io.mewbase.cqrs;


import io.mewbase.binders.BinderStore;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.impl.QueryManagerImpl;


import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by Tim on 10/01/17.
 */
public interface QueryManager  {

    /**
     * Factory method for QueryManager.
     * Given a Binder return a new Instance of a QueryManger
     *
     * @param BinderStore - The BinderStore on which to execute Query
     * @return
     */
    static QueryManager instance(BinderStore store)  {
        return new QueryManagerImpl(store);
    }

    QueryBuilder queryBuilder();

    Optional<Query> getQuery(String queryName);

    Stream<Query> getQueries();

    Stream<KeyVal<String, BsonObject>> execute(String queryName, BsonObject context);

}
