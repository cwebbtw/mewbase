package io.mewbase.cqrs;

import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;


import java.util.function.BiPredicate;
import java.util.stream.Stream;

/**
 * Created by tim on 10/01/17.
 * re-created by Nige on 19/10/17.
 */

public interface Query {

    String getName();

    Binder getBinder();

    /**
     * Return the function that can filter the documents in this binder in a context
     * Params are
     * Context Object - passed in when the Query is executed
     * KeyVal - String - Document ID
     * KeyVal - BsonObject - Document contents.
     * @return
     */
    BiPredicate<BsonObject, KeyVal<String, BsonObject>> getQueryFilter();

    Stream<KeyVal<String, BsonObject>> execute(BsonObject params);

}
