package io.mewbase.cqrs;

import io.mewbase.bson.BsonObject;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by tim on 07/01/17.
 */
public interface QueryBuilder {

    QueryBuilder named(String queryName);

    QueryBuilder from(String binderName);

    QueryBuilder filteredBy(Predicate<BsonObject> documentFilter);

    QueryBuilder selectedBy(Function<BsonObject, String> idSelector);

    Query create();
}
