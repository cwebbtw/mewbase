package io.mewbase.projection;

import io.mewbase.binders.BinderStore;
import io.mewbase.eventsource.EventSource;
import io.mewbase.projection.impl.ProjectionManagerImpl;

import java.util.stream.Stream;

/**
 * ProjectionFactory can be used to get projection builders and wires them to the enclosed
 * event source and binder store
 */
public interface ProjectionManager {

    /**
     * Factory method for Projection Manager.
     * Given an EventSource and a BinderStore return the ability to to make ProjectionBuilders
     *
     * @param source - The EventSource
     * @param store  - A BinderStore to wire the projection to
     * @return
     * @throws Exception
     */
    static ProjectionManager instance(EventSource source, BinderStore store) throws Exception {
        return new ProjectionManagerImpl(source,store);
    }


    /**
     * Projections are complex so wiring is done through builders
     * @return A new builder that can be used to assemble a Projection
     */
    ProjectionBuilder builder();


    /**
     * Does this Factory contain a projection with the given name
     * @param projectionName
     * @return
     */
    boolean isProjection(String projectionName);


    /**
     * Return as a stream the list of projection names.
     * @return
     */
    Stream<String> projectionNames();

    /**
     * Stops all of the current projections.
     */
    void stopAll();
}
