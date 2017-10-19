package io.mewbase.cqrs;

import io.mewbase.binders.Binder;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.impl.CommandManagerImpl;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;

import java.util.concurrent.CompletableFuture;

/**
 * Created by Tim on 10/01/17.
 */
public interface QueryManager  {

    /**
     * Factory method for QueryManager.
     * Given a Binder return a new Instance of a QueryManger
     *
     * @param binder - The Binder on which to execute Query
     * @return
     */
    static QueryManager instance(Binder binder)  {
        // Todo 
        return null; //new QueryManagerImpl(binder);
    }
    

    public QueryBuilder queryBuilder();

    public CompletableFuture<BsonObject> execute(BsonObject context);


}
