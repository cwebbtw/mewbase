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
import io.mewbase.util.FallibleFuture;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;


public class ProjectionManagerImpl implements ProjectionManager, FallibleFuture {

    private final static Logger log = LoggerFactory.getLogger(ProjectionManager.class);

    public static final String METRICS_NAME = "mewbase.projection";

    private final EventSource source;
    private final BinderStore store;

    public static final String PROJ_STATE_BINDER_NAME = "mewbase_proj_state";
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
    CompletableFuture<Projection> createProjection(final String projectionName,
                                final String channelName,
                                final String binderName,
                                final Function<Event, Boolean> eventFilter,
                                final Function<Event, String> docIDSelector,
                                final BiFunction<BsonObject, Event, BsonObject> projectionFunction) {

        // instrument this projection
        List<Tag> tag = Arrays.asList(Tag.of("name", projectionName));
        Counter projectionCounter = Metrics.counter( METRICS_NAME, tag );

        EventHandler eventHandler =  (Event event) -> {
            try {
                if (eventFilter.apply(event)) {
                    String docID = docIDSelector.apply(event);
                    if (docID == null) {
                        log.error("In projection " + projectionName + " document id selector returned null");
                    } else {
                        try {
                            projectionCounter.increment();
                            executeProjection(projectionName, binderName, docID, projectionFunction, event);
                        } catch (Exception exp) {
                            log.error("Projection failed to execute - Stopping" +
                                    " Projection:" + projectionName +
                                    " Binder:" + binderName +
                                    " Document ID:" + docID, exp);
                            projections.get(projectionName).stop();
                        }
                    }
                }
            } catch (Exception exp) {
                log.error("Projection event handler failed", exp);
            }
        };


        final CompletableFuture<Subscription> subs = subscribeFromLastKnownEvent(projectionName,channelName,eventHandler);
        return subs.thenApply( subscription -> {
            final ProjectionImpl proj = new ProjectionImpl(projectionName,subscription);
            projections.put(projectionName,proj);
            return proj;
        });
    }


    private BsonObject executeProjection(String projectionName,
                                     String binderName,
                                     String docID,
                                     BiFunction<BsonObject, Event, BsonObject> projectionFunction,
                                     Event event ) throws Exception {

        // as sequential for reasons of preserving sanity
        final Binder docBinder = store.open(binderName);
        final BsonObject inputDoc = docBinder.get(docID).get();
        final BsonObject validDoc = (inputDoc == null) ? new BsonObject() : inputDoc;
        final BsonObject outputDoc = projectionFunction.apply(validDoc, event);
        final BsonObject projStateDoc = new BsonObject().put(EVENT_NUM_FIELD, event.getEventNumber());

        // if doc binder fails then the state fails
        docBinder.put(docID, outputDoc).get();
        try {
            stateBinder.put(projectionName, projStateDoc).get();
        } catch (Exception exp) {
            // Doc succeeded and state failed
            log.error("State Write failed possible sync error",  exp);
            throw exp;
        }
        return outputDoc;
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


    private CompletableFuture<Subscription> subscribeFromLastKnownEvent(String projectionName, String channelName, EventHandler eventHandler) {
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
            return FallibleFuture.failedFuture(exp);
        }

    }

}
