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
import io.mewbase.server.Server;
import io.mewbase.server.MewbaseOptions;

import java.util.function.Consumer;

/**
 * Simple Shopping Basket when the Basket is an Aggregate projection over the
 * order events
 *
 * Created by tim on 08/11/16.
 * Updated by Nige on 25/10/17
 */

public class ShoppingBasketExample {


    public static void main(String[] args) throws Exception {

        // In order to run a projection set up an EventSource and BinderStore.
        EventSource src = new NatsEventSource();
        BinderStore store = BinderStore.instance();
        ProjectionManager mgr = ProjectionManager.instance(src,store);

        final String ORDERS_EVENT_CHANNEL = "OrderEventsChannel";
        final String BASKETS_BINDER_NAME = "Baskets";
        final String PROJECTION_NAME = "MaintainBasket";

        final String EVENT_TYPE_KEY = "EventType";
        final String ADD_EVENT = "AddEvent";
        final String REMOVE_EVENT = "RemoveEvent";

        final String BASKET_ID_KEY = "BasketID";
        final String BASKET_ID = "Basket-i834582267567568947";

        // Register a projection that will respond to add_item events and increase/decrease the
        // quantity of the item in the basket
        mgr.builder().named(PROJECTION_NAME)
                        // Channel to listen to
                        .projecting(ORDERS_EVENT_CHANNEL)
                        // filter for the addItem events
                        .filteredBy(evt -> {
                            final String eventType =  evt.getBson().getString(EVENT_TYPE_KEY);
                            return eventType.equals(ADD_EVENT) || eventType.equals(REMOVE_EVENT);
                        })
                        // event filter
                        .onto(BASKETS_BINDER_NAME)
                        // find the Basket in the binder
                        .identifiedBy(evt -> evt.getBson().getString(BASKET_ID_KEY))
                        // projection
                        .as( (BsonObject basket, Event evt) ->  {
                            final String eventType = evt.getBson().getString(EVENT_TYPE_KEY);
                            final String productID = evt.getBson().getString("productID");
                            final int quantity = evt.getBson().getInteger("quantity");

                            // get the current quantity from the basket
                            final Integer prevQuantity = basket.getInteger(productID,0);
                            if (eventType.equals(ADD_EVENT)) basket.put(productID,prevQuantity+quantity);
                            if (eventType.equals(REMOVE_EVENT)) basket.put(productID,prevQuantity-quantity);
                            return basket;
                        } ).create();


        //************************** Client Side Setup ******************************

        // set up a sink to send events to the projection
        EventSink sink = new NatsEventSink();

        // Create some add/remove events
        BsonObject event = new BsonObject().put(BASKET_ID_KEY, BASKET_ID);
        BsonObject addEvent = event.copy().put(EVENT_TYPE_KEY, ADD_EVENT);
        BsonObject remEvent = event.copy().put(EVENT_TYPE_KEY, REMOVE_EVENT);

        sink.publish(ORDERS_EVENT_CHANNEL, addEvent.copy().put("productID", "prod1234").put("quantity", 2));
        sink.publish(ORDERS_EVENT_CHANNEL, addEvent.copy().put("productID", "prod2341").put("quantity", 1));
        sink.publish(ORDERS_EVENT_CHANNEL, addEvent.copy().put("productID", "prod5432").put("quantity", 3));
        sink.publish(ORDERS_EVENT_CHANNEL, remEvent.copy().put("productID", "prod5432").put("quantity", 1));

        Thread.sleep(100);

        // Consumer of the Basket
        Consumer<BsonObject> basketConsumer = basketState ->
                System.out.println("Basket State is :" + basketState);

        Binder fridgeStatusBinder = store.get(BASKETS_BINDER_NAME).get();
        fridgeStatusBinder.get(BASKET_ID).thenAccept( basketConsumer );

        // close the resources
        sink.close();
        src.close();
    }

}
