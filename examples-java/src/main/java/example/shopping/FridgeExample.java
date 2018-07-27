package example.shopping;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;

import io.mewbase.projection.ProjectionManager;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


/**
 * Fridge Example
 * <p>
 * We assume that we have a number of Fridges (Refrigerators) that can raise IOT style events.
 * Each time the Fridge produces an event we Publish the event as a BsonObject via an EventSink.
 * For each Fridge we maintain a Document in the 'fridges' Binder that reflects the current state
 * of each of the fridges that are sending out events.
 * <p>
 * Some extension that may be informative  ...
 * <p>
 * 1) Introduce events from fridges with new Ids and check these also appear the Binder.
 * 2) Introduce a new event that reflects the temperature of the fridge and integrate this into
 * the current projection, or make a new projection to handle the new status event.
 * <p>
 * Created by Nige on 17/05/17
 * Updated by Nige on 25/10/17
 * Updated by Nige on 16/4/18
 */

public class FridgeExample {

    private static final String FRIDGE_EVENT_CHANNEL_NAME = "FridgeStatusChannel";
    private static final String FRIDGE_BINDER_NAME = "FridgeBinder";

    public static void main(String[] args) throws Exception {

        //************************** Server Side Setup ******************************
        // In order to run a projection set up an EventSource and BinderStore.
        try (EventSource src = EventSource.instance(); // a source raw event data
             BinderStore store = BinderStore.instance(); // holds a collection of documents. each document represents the state of a fridge
             EventSink sink = EventSink.instance(); // a consumer of raw event data. it writes events to the event source
             ProjectionManager mgr = ProjectionManager.instance(src, store) /* reacts to incoming events, possibly affecting the binder state */) {

            // Create a projection by using a fluent builder from the manager.
            mgr.builder().named("maintain_fridge_status")             // projection name
                    // channel name
                    .projecting(FRIDGE_EVENT_CHANNEL_NAME)
                    // binder name
                    .onto(FRIDGE_BINDER_NAME)
                    // event filter
                    .filteredBy(evt -> evt.getBson().getString("eventType").equals("doorStatus"))
                    // document id selector; how to obtain the doc id from the event Bson
                    .identifiedBy(evt -> evt.getBson().getString("fridgeID"))
                    // projection to update the fridge state with the given event
                    .as((BsonObject fridgeState, Event evt) -> {
                        final String doorStatus = evt.getBson().getString("status");
                        fridgeState.put("door", doorStatus);
                        return fridgeState;
                    })
                    .create();

            // Send some open/close events for this fridge
            BsonObject event = new BsonObject().put("fridgeID", "f1").put("eventType", "doorStatus");
            Long eventNumber = sink.publishSync(FRIDGE_EVENT_CHANNEL_NAME, event.copy().put("status", "open"));
            // CompletableFuture<Long> eventNumberFuture = sink.publishAsync(FRIDGE_EVENT_CHANNEL_NAME, event.copy().put("status", "open"));

            // wait for the projection to fire
            Thread.sleep(200);

            // Consumer of the Fridge Status Document
            Consumer<BsonObject> statusDocumentConsumer =
                    fridgeStateDoc -> System.out.println("Fridge State is :" + fridgeStateDoc);
            Binder fridgeStatusBinder = store.get(FRIDGE_BINDER_NAME).get();
            fridgeStatusBinder.get("f1").thenAccept(statusDocumentConsumer);

            // Shut that door
            sink.publishSync(FRIDGE_EVENT_CHANNEL_NAME, event.copy().put("status", "shut"));

            // wait for the projection again
            Thread.sleep(200);

            // Now get the fridge state again
            fridgeStatusBinder.get("f1").thenAccept(statusDocumentConsumer);
        }
    }

}
