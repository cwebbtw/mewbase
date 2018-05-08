package io.mewbase.projection.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;
import io.mewbase.util.CanFailFutures;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 28/11/16.
 */
public class ProjectionBuilderImpl implements ProjectionBuilder, CanFailFutures {

    private final ProjectionManagerImpl factory;

    private String projectionName;
    private String channelName;
    private String binderName;

    private Function<Event, Boolean> eventFilter = doc -> true;
    private Function<Event, String> docIDSelector;
    private BiFunction<BsonObject, Event, BsonObject> projectionFunction;
    private Optional<String> outputEventChannel = Optional.empty();


    ProjectionBuilderImpl(ProjectionManagerImpl factory) {
        this.factory = factory;
    }

    @Override
    public ProjectionBuilder named(String projectionName) {
        this.projectionName = projectionName;
        return this;
    }

    @Override
    public ProjectionBuilder projecting(String channelName) {
        this.channelName = channelName;
        return this;
    }

    @Override
    public ProjectionBuilder filteredBy(Function<Event, Boolean> eventFilter) {
        this.eventFilter = eventFilter;
        return this;
    }

    @Override
    public ProjectionBuilder onto(String binderName) {
        this.binderName = binderName;
        return this;
    }

    @Override
    public ProjectionBuilder identifiedBy(Function<Event, String> docIDSelector) {
        this.docIDSelector = docIDSelector;
        return this;
    }

    @Override
    public ProjectionBuilder as(BiFunction<BsonObject, Event, BsonObject> projectionFunction) {
        this.projectionFunction = projectionFunction;
        return this;
    }


    @Override
    public CompletableFuture<Projection> create()  {

        if (projectionName == null) {
            return CanFailFutures.failedFuture(new IllegalStateException("Please specify a projection name"));
        }
        if (channelName == null) {
            return CanFailFutures.failedFuture(new IllegalStateException("Please specify a channel name"));
        }
        if (binderName == null) {
            return CanFailFutures.failedFuture(new IllegalStateException("Please specify a binder name"));
        }
        if (eventFilter == null) {
            return CanFailFutures.failedFuture(new IllegalStateException("Please specify an event filter"));
        }
        if (docIDSelector == null) {
            return CanFailFutures.failedFuture(new IllegalStateException("Please specify a document ID filter"));
        }
        if (projectionFunction == null) {
            return CanFailFutures.failedFuture(new IllegalStateException("Please specify a projection function"));
        }

        // Check for name collisions
        if ( factory.isProjection(projectionName) ) {
            return CanFailFutures.failedFuture(new IllegalStateException("Projection name is already being used " + projectionName));
        }

        // TODO if the BinderStore is streaming ensure the input channel is not the output channel

        // Use the factory to internal create the Projection.
        return factory.createProjection(projectionName,
                                        channelName,
                                        binderName,
                                        eventFilter,
                                        docIDSelector,
                                        projectionFunction);
    }

}
