package example.rest;


import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.CommandManager;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.projection.ProjectionManager;
import io.mewbase.rest.RestServiceAdaptor;

/**
 * Create a very simple rest service with the following endpoints
 *
 * POST : <host:port>/start
 * Fires a start event at the event source on the "commands" channel.
 * A projection is running so this will create a new document that is named for the current time in the "events" binder
 *
 * POST : <host:port>/stop
 * Fires a stop event at the event source on the "commands" channel.
 * A projection is running so this will create a new document that is named for the current time in the "events" binder
 *
 * GET : <host:port>/binders/events
 * Will list all the documents that have been created in the events Binder like
 *[
 "2018-01-18T14:36:28.742147200Z",
 "2018-01-18T14:36:39.494138600Z",
 "2018-01-18T14:32:55.156797700Z",
 "2018-01-18T14:36:39.749554100Z",
 "2018-01-18T14:32:25.846560900Z",
 "2018-01-18T14:33:05.957730300Z",
 "2018-01-18T14:31:20.414509Z"
 ]
 *
 * GET : <host:port>/binders/events/<eventName>
 * Use the event name that represents the event (from the list returned from the above events call)
 * to produce the contents if a document representing that event e.g.
 * {"eventNumber":7,"command":"stop"}
 *
 */
public class RestService {

    static final String commandChannel = "commands";

    public static void main(String [] args) throws Exception {

        // Wire together the top level objects
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
        // projection for historic documents
        createProjection( projMgr);

        // and start
        restAdapter.start();
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
