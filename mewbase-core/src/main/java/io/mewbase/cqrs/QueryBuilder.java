package io.mewbase.cqrs;

import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;

import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by tim on 07/01/17.
 */
public interface QueryBuilder {

    QueryBuilder named(String queryName);

    QueryBuilder from(String binderName);

    QueryBuilder filteredBy(BiPredicate<BsonObject, KeyVal<String,BsonObject>> documentFilter);

    Query create();
}
