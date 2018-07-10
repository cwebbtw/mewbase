package io.mewbase.rest

import java.util.function.BiPredicate
import java.util.{Optional, stream}

import io.mewbase.binders.{Binder, KeyVal}
import io.mewbase.bson.BsonObject
import io.mewbase.cqrs.{Query, QueryBuilder, QueryManager}

import scala.collection.mutable.ListBuffer

case class StubQuery(name: String, results: Map[String, BsonObject]) extends Query { query =>
  val executedContexts = new ListBuffer[BsonObject]

  override def getName: String = name

  override def getBinder: Binder = ???

  /**
    * Return the function that can filter the documents in this binder in a context
    * Params are
    * Context Object - passed in when the Query is executed
    * KeyVal - String - Document ID
    * KeyVal - BsonObject - Document contents.
    *
    * @return
    */
  override def getQueryFilter: BiPredicate[BsonObject, KeyVal[String, BsonObject]] = ???

  override def execute(params: BsonObject): stream.Stream[KeyVal[String, BsonObject]] = {
    executedContexts += params
    val streamBuilder = stream.Stream.builder[KeyVal[String, BsonObject]]

    results.foreach({ case (key, value) => streamBuilder.accept(new KeyVal(key, value)) })

    streamBuilder.build()
  }

  def stubQueryManager(): QueryManager =
    new QueryManager {
      override def queryBuilder(): QueryBuilder = ???

      override def getQuery(queryName: String): Optional[Query] =
        if (name == queryName)
          Optional.of(query)
        else
          Optional.empty()

      override def getQueries: stream.Stream[Query] = stream.Stream.of(List(query): _*)

      override def execute(queryName: String, context: BsonObject): stream.Stream[KeyVal[String, BsonObject]] = {
        query.execute(context)
      }
    }
}
