package io.mewbase.cqrs;

import io.mewbase.bson.BsonObject;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by tim on 07/01/17.
 */
public interface QueryBuilder {

    QueryBuilder named(String queryName);

    QueryBuilder from(String binderName);

    QueryBuilder selectedBy(Function<BsonObject, Set<String>> idSelector);

    QueryBuilder filteredBy(Predicate<BsonObject> documentFilter);

    Query create();
}
