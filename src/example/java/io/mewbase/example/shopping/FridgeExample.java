package io.mewbase.example.shopping;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSource;

import io.mewbase.projection.ProjectionManager;


import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


/**
 * Fridge Example
 *
 * We assume that we have a number of Fridges (Refrigerators) that can raise IOT style events.
 * Each time the Fridge produces an event we Publish the event as a BsonObject via an EventSink.
 * For each Fridge we maintain a Document in the 'fridges' Binder that reflects the current state
 * of each of the fridges that are sending out events.
 *
 * Some extension that may be informative  ...
 *
 * 1) Introduce events from fridges with new Ids and check these also appear the Binder.
 * 2) Introduce a new event that reflects the temperature of the fridge and integrate this into
 * the current projection, or make a new projection to handle the new status event.
 *
 * The example depends on a running instance of NatsStreaming service. See the instructions
 * here https://github.com/Tesco/mewbase
 *
 * Created by Nige on 17/05/17.
 * Updated by Nige on 25/10/17
 */

public class FridgeExample {

    public static void main(String[] args) throws Exception {

        // In order to run a projection set up an EventSource and BinderStore.
        EventSource src = EventSource.instance();
        BinderStore store = BinderStore.instance();
        ProjectionManager mgr = ProjectionManager.instance(src,store);

        final String FRIDGE_EVENT_CHANNEL_NAME = "FridgeStatusChannel";
        final String FRIDGE_BINDER_NAME = "FridgeBinder";


        // Create a projection by using a fluent builder from the manager.
        mgr.builder().named("maintain_fridge_status")             // projection name
                // channel name
                .projecting(FRIDGE_EVENT_CHANNEL_NAME)
                // event filter
                .filteredBy(evt -> evt.getBson().getString("eventType").equals("doorStatus"))
                // binder name
                .onto(FRIDGE_BINDER_NAME)
                // document id selector; how to obtain the doc id from the event Bson
                .identifiedBy(evt -> evt.getBson().getString("fridgeID"))
                // projection to update the fridge state with the given event
                .as( (BsonObject fridgeState, Event evt) ->  {
                        final String doorStatus = evt.getBson().getString("status");
                        fridgeState.put("door", doorStatus);
                        return fridgeState;
                } )
                .create();



        //************************** Client Side Setup ******************************

        // set up a sink to send events to the projection
        EventSink sink = new NatsEventSink();

        // Send some open close events for this fridge
        BsonObject event = new BsonObject().put("fridgeID", "f1").put("eventType", "doorStatus");
        CompletableFuture<BsonObject> fut = sink.publishAsync(FRIDGE_EVENT_CHANNEL_NAME, event.copy().put("status", "open"));

        // Consumer of the Docuemnt
        Consumer<BsonObject> statusDocumentConsumer = fridgeStateDoc ->
                System.out.println("Fridge State is :" + fridgeStateDoc);

        Binder fridgeStatusBinder = store.get(FRIDGE_BINDER_NAME).get();
        fridgeStatusBinder.get("f1").thenAccept( statusDocumentConsumer );

        // Shut that door
        sink.publishSync(FRIDGE_EVENT_CHANNEL_NAME,event.copy().put("status", "shut"));

        // wait for the projection to fire
        Thread.sleep(100);

        // Now get the fridge state again
        fridgeStatusBinder.get("f1").thenAccept( statusDocumentConsumer );

        // close the resources
        sink.close();
        src.close();

    }

}
