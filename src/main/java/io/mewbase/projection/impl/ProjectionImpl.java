package io.mewbase.projection.impl;


import io.mewbase.eventsource.Subscription;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ProjectionImpl implements Projection {

    private final static Logger log = LoggerFactory.getLogger(ProjectionImpl.class);

    final String name;
    final Subscription subs;

    public ProjectionImpl(String name, Subscription subs) {
        this.name = name;
        this.subs = subs;
        log.info("Projection " + name + " created.");
    }

    @Override
    public String getName() {
        return name;
    }



    @Override
    public void stop() {
        subs.close();
        log.info("Projection " + name + " closing down.");
    }

}
