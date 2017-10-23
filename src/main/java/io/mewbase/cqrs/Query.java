package io.mewbase.cqrs;

import io.mewbase.binders.Binder;
import io.mewbase.bson.BsonObject;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by tim on 10/01/17.
 * re-created by Nige on 19/10/17.
 */

public interface Query {

    String getName();

    Binder getBinder();

    Predicate<BsonObject> getDocumentFilter();

    Function<BsonObject, String> getIdSelector();

    interface Result {
        String getId();
        BsonObject getDocument();
    }

    Stream<Result> execute(BsonObject params);

}
