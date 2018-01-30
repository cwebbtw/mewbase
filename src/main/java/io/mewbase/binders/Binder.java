package io.mewbase.binders;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import java.util.concurrent.CompletableFuture;

import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by tim on 29/12/16.
 *
 * Binders are in essence persistent maps from Document Id's to Documents in the following form
 *
 * Key - String - Document Ids
 * Value - BsonObject - Document
 *
 */
public interface Binder {

    /**
     * Get the name of this binder
     * @return The binders name
     */
    String getName();

    /**
     * Get a document with the given document id
     *
     * @param id the name of the document within the binder
     * @return a CompleteableFuture of the document
     */
    CompletableFuture<BsonObject> get(String id);

    /**
     * Put a document at the given id - overwrites any previous document stored under the ID
     *
     * @param id  the name of the document within the binder
     * @param doc the document to save
     * @return
     */
    CompletableFuture<Void> put(String id, BsonObject doc);

    /**
     * Delete a document from a binder
     *
     * @param id the name of the document within the binder
     * @return a CompleteableFuture with a Boolean set to true if successful
     */
    CompletableFuture<Boolean> delete(String id);

    /**
     * Get all of the Documents and their ID's contained in this binder.
     *
     * @return A stream of all of the ids and documents in the binder
     */
    Stream<KeyVal<String, BsonObject>> getDocuments();

    /**
     * Get all of the documents in the Binder that match the filter.
     *
     * KeyVal is the Document ID and Contents to apply the filter to.
     *
     * @return A stream of the matching ids and documents in the binder.
     */
    Stream<KeyVal<String, BsonObject>> getDocuments( Predicate<KeyVal<String,BsonObject>> filter);


    /**
     * Set an EventSink and a channel making this binder stream documents that are put
     * into this binder onto the given channel
     */
    Boolean setStreaming(final EventSink sink, final String channel);


    /**
     * Check if this Binder is streaming docs to an EventSink
     */
    Boolean isStreaming();


}
