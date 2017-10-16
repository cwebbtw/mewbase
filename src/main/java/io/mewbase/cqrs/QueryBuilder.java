package io.mewbase.cqrs;

import io.mewbase.bson.BsonObject;

import java.util.function.BiFunction;

/**
 * Created by tim on 07/01/17.
 */
public interface QueryBuilder {

    QueryBuilder from(String binderName);

    QueryBuilder documentFilter(BiFunction<BsonObject, BsonObject, Boolean> documentFilter);

    Query create();
}
