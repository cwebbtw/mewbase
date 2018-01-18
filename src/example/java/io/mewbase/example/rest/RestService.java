package io.mewbase.example.rest;


import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.CommandManager;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.projection.ProjectionManager;
import io.mewbase.rest.RestServiceAdaptor;


public class RestService {

    static final String commandChannel = "commands";

    public static void main(String [] args) throws Exception {

        // Plug wire together the top level  objects
        final EventSink eventSink = EventSink.instance();
        final EventSource eventSource = EventSource.instance();
        final BinderStore store = BinderStore.instance();
        final CommandManager cmdMgr = CommandManager.instance(eventSink);
        final ProjectionManager projMgr = ProjectionManager.instance(eventSource, store);
        final RestServiceAdaptor restAdapter = RestServiceAdaptor.instance();

        // create and install commands
        restAdapter.exposeCommand(cmdMgr,createCommand(cmdMgr,"start"));
        restAdapter.exposeCommand(cmdMgr,createCommand(cmdMgr,"stop"));
        // make the binders available on RestEnd Points
        restAdapter.exposeGetDocument(store);
        // projection for history documents
        createProjection( projMgr);
    }


    private static String createCommand(CommandManager cmdMgr, String commandName) {
         return cmdMgr.commandBuilder().
                 named(commandName).
                 emittingTo(commandChannel).
                 as( params -> new BsonObject().put("command",commandName)).
                 create().getName();
    }

   private static void createProjection(ProjectionManager projMgr) {
        projMgr.builder().
            named("documentsForCommand").
            onto("events").
            projecting(commandChannel).
            identifiedBy( evt -> evt.getInstant().toString()).
            as( (doc, evt) -> evt.getBson().put("eventNumber",evt.getEventNumber()))
            .create();
   }

}
