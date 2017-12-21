package io.mewbase.binders;

import io.mewbase.bson.BsonObject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by tim on 29/12/16.
 *
 * Binders are in essence persistent maps from Document Id's to Documents in teh following form
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
     * Get all of the documents in the Binder given a KeySet and a Content based filter.
     *
     * Finds all the documents matching the keySet.
     * If then keySet is empty then match all of the keys.
     *
     * Then apply the filter Predicate to the items that match the key set.
     *
     * @return A stream of the matching ids and documents in the binder.
     */
    Stream<KeyVal<String, BsonObject>>
                getDocuments(Set<String> keySet, Predicate<BsonObject> filter);

}
