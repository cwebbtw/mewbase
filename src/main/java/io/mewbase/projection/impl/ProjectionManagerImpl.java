package io.mewbase.projection.impl;


import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;
import io.mewbase.projection.ProjectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;


public class ProjectionManagerImpl implements ProjectionManager {

    private final static Logger log = LoggerFactory.getLogger(ProjectionManager.class);

    private final EventSource source;
    private final BinderStore store;

    public static final String PROJ_STATE_BINDER_NAME = "mewbase.proj.state";
    public static final String EVENT_NUM_FIELD = "eventNum";

    private final Binder stateBinder;

    private final Map<String, ProjectionImpl> projections = new ConcurrentHashMap<>();


    public ProjectionManagerImpl(EventSource source, BinderStore store) throws Exception {
        this.source = source;
        this.store = store;
        this.stateBinder = store.open(PROJ_STATE_BINDER_NAME);
    }


    @Override
    public ProjectionBuilder builder() {
       return new ProjectionBuilderImpl(this);
    }

    /**
     * Instance a projection and register it with the factory
     * @param projectionName
     * @param channelName
     * @param binderName
     * @param eventFilter
     * @param docIDSelector
     * @param projectionFunction
     * @return
     */
    Projection createProjection(final String projectionName,
                                final String channelName,
                                final String binderName,
                                final Function<Event, Boolean> eventFilter,
                                final Function<Event, String> docIDSelector,
                                final BiFunction<BsonObject, Event, BsonObject> projectionFunction) {

        EventHandler eventHandler =  (Event event) -> {
            if (eventFilter.apply(event)) {
                String docID = docIDSelector.apply(event);
                if (docID == null) {
                    log.error("In projection " + projectionName + " document id selector returned null");
                } else {
                    Binder docBinder = store.open(binderName);
                    docBinder.get(docID).whenComplete((final BsonObject inputDoc, final Throwable innerExp) -> {
                        // Something broke in the store/binder dont try to apply the projection
                        // and log the error don't apply the
                        if (innerExp != null) {
                            log.error("Attempt to read document from store failed in " +
                                        " Projection:" + projectionName +
                                        " Binder:" + binderName +
                                        " Document ID:" + docID, innerExp);
                        } else {
                            // Check that the document exists and provide an empty one if it doesnt.
                            // should never pass a null to the projection apply function
                            final BsonObject validDoc = (inputDoc == null) ? new BsonObject() : inputDoc;

                            BsonObject outputDoc = projectionFunction.apply(validDoc, event);
                            BsonObject projStateDoc = new BsonObject().put(EVENT_NUM_FIELD, event.getEventNumber());

                            // Set up a handler so that if the projection fails because the state
                            // store fails for any reason then attempt to rewind a possible
                            // partial or complete failure. And then finally stop the projection
                            // on that assumption that the storage has failed
                            Function<Throwable, Void> rewindHandler = (final Throwable thrbl) -> {
                                // assuming one of the writes failed that it is extremely likely that the rewind
                                // will succeed for similar reasons and if both failed then rewind will be idempotent.
                                docBinder.put(docID, validDoc);
                                final long previousEvent = Math.max(0,event.getEventNumber()-1);
                                BsonObject prevStateDoc = new BsonObject().put(EVENT_NUM_FIELD, previousEvent);
                                stateBinder.put(projectionName, prevStateDoc);
                                log.error("Projection write failed and rewound at " +
                                        "Projection:" + projectionName +
                                        " Binder:" + binderName +
                                        " Document ID:" + docID, thrbl);
                                log.error("Hence stopping projection.");
                                projections.get(projectionName).stop();
                                return null;
                            };

                            // Now attempt Attempt to write both the update to the document and the state
                            CompletableFuture<Void> stateWrite = stateBinder.put(projectionName, projStateDoc);
                            CompletableFuture<Void> binderWrite = docBinder.put(docID, outputDoc);


                            // If either fails then rewind.
                            binderWrite.exceptionally(rewindHandler);
                            stateWrite.exceptionally(rewindHandler);
                        }
                    });
                }
            }
        };

        Subscription subs = subscribeFromLastKnownEvent(projectionName,channelName,eventHandler);

        // register it with the manager
        ProjectionImpl proj = new ProjectionImpl(projectionName,subs);
        projections.put(projectionName,proj);
        return proj;
    }

    @Override
    public boolean isProjection(String projectionName) {
        return projections.keySet().contains(projectionName);
    }

    @Override
    public Stream<String> projectionNames() {
        return projections.keySet().stream() ;
    }

    @Override
    public void stopAll() { projections.forEach( (name, proj) -> proj.stop() ); }


    private Subscription subscribeFromLastKnownEvent(String projectionName, String channelName, EventHandler eventHandler) {
        try {
            final BsonObject stateDoc = stateBinder.get(projectionName).get();
            if (stateDoc == null) {
                log.info("Projection " + projectionName + " subscribing from start of channel " + channelName);
                return source.subscribeAll(channelName, eventHandler);
            } else {
                Long nextEvent = stateDoc.getLong(EVENT_NUM_FIELD) + 1;
                log.info("Projection " + projectionName + " subscribing from event number " + nextEvent);
                return source.subscribeFromEventNumber(channelName, nextEvent, eventHandler);
            }
        } catch (Exception exp) {
            log.error("Failed to recover last known state of the projection " + projectionName, exp);
        }
        return null;
    }

}
