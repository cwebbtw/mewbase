package io.mewbase.rest

import java.util.concurrent.CompletableFuture
import java.util.function.Predicate
import java.util.{Optional, stream}
import java.{lang, util}

import io.mewbase.binders.{Binder, BinderStore, KeyVal}
import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.EventSink

import scala.collection.mutable

case class StubBinder(name: String) extends Binder { binder =>

  val documents: mutable.Map[String, BsonObject] =
    new mutable.HashMap[String, BsonObject]()

  /**
    * Get the name of this binder
    *
    * @return The binders name
    */
  override def getName: String = name

  /**
    * Get a document with the given document id
    *
    * @param id the name of the document within the binder
    * @return a CompleteableFuture of the document
    */
  override def get(id: String): CompletableFuture[BsonObject] =
    CompletableFuture.supplyAsync(() => documents(id))

  /**
    * Put a document at the given id - overwrites any previous document stored under the ID
    *
    * @param id  the name of the document within the binder
    * @param doc the document to save
    * @return a CompleteableFuture with a Boolean set to true
    */
  override def put(id: String, doc: BsonObject): CompletableFuture[lang.Boolean] =
    CompletableFuture.supplyAsync(() => documents.put(id, doc).isDefined)

  /**
    * Delete a document from a binder
    *
    * @param id the name of the document within the binder
    * @return a CompleteableFuture with a Boolean set to true
    */
  override def delete(id: String): CompletableFuture[lang.Boolean] =
    CompletableFuture.supplyAsync(() => documents.remove(id).isDefined)

  /**
    * Count up all the documents in this Binder as fast as you like.
    *
    * @return the total number of documents in this binder.
    */
  override def countDocuments(): lang.Long =
    documents.size.toLong

  /**
    * Get all of the Documents and their ID's contained in this binder.
    *
    * @return A stream of all of the ids and documents in the binder
    */
  override def getDocuments: stream.Stream[KeyVal[String, BsonObject]] =
    util.Arrays.stream(documents.map { case (key, value) => new KeyVal[String, BsonObject](key, value) }.toArray)

  /**
    * Get all of the documents in the Binder that match the filter.
    *
    * KeyVal is the Document ID and Contents to apply the filter to.
    *
    * @return A stream of the matching ids and documents in the binder.
    */
  override def getDocuments(filter: Predicate[KeyVal[String, BsonObject]]): stream.Stream[KeyVal[String, BsonObject]] = {
    val docs = documents.map { case (key, value) => new KeyVal[String, BsonObject](key, value) }
    util.Arrays.stream(docs.filter(filter.test).toArray)
  }

  /**
    * Set an EventSink and a channel making this binder stream documents that are put
    * into this binder onto the given channel
    */
  override def setStreaming(sink: EventSink, channel: String): lang.Boolean = false

  /**
    * Check if this Binder is streaming docs to an EventSink
    */
  override def isStreaming: lang.Boolean = false

  val binderStore: BinderStore = new BinderStore {/**
    * Return a stream of all of the names of the binders
    *
    * @return a stream of all of the current binder names.
    */
  override def binderNames(): stream.Stream[String] =
    java.util.stream.Stream.of(List(name):_*)

    /**
      * Return a stream of the Binders so that maps / filters can be applied.
      *
      * @return a stream of all of the current binders
      */
    override def binders(): stream.Stream[Binder] =
      java.util.stream.Stream.of(List(binder):_*)

    /**
      * Delete a binder from the store
      *
      * @param  name of  binder
      * @return a CompleteableFuture with a Boolean set to true if successful
      */
    override def delete(name: String): lang.Boolean = false

    /**
      * Get a Binder with the given name
      *
      * @param  name of the document within the binder
      * @return a CompletableFuture of the binder or a failed future if the binder doesnt exist.
      */
    override def get(name: String): Optional[Binder] =
      if (name != binder.name)
        Optional.empty()
      else
        Optional.of(binder)

    /**
      * Open a new binder of the given name.
      *
      * If the binder doesnt already exist the binder wil be created.
      *
      * @param name of the Binder to open or create and open
      * @return succesfull  if Binder is created otherwise complet
      */
    override def open(name: String): Binder =
      if (name != binder.name)
        throw new IllegalArgumentException(s"Stub binder store only supports open for ${binder.name}, attempted with $name")
      else
        binder

    override def close(): Unit = ()
  }
}
