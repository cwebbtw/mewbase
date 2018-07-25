package io.mewbase.eventsource.impl.hbase;


import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.impl.EventDispatcher;

import io.mewbase.eventsource.impl.file.FileEvent;

import io.netty.buffer.Unpooled;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class HBaseEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(HBaseEventSubscription.class);

    private final Future reader;

    private final EventDispatcher<Event> dispatcher;

    private final Table channelTable;

    public final CompletableFuture<Subscription> initialisingFuture = new CompletableFuture<>();

    private Boolean closing = false;


    public HBaseEventSubscription(final Table table, final long firstEventNumber, final EventHandler eventHandler) {

        this.channelTable = table;

        // a FileEvent is an Event hence i -> i is identity.
        this.dispatcher = new EventDispatcher<>( i -> i, eventHandler );

        reader = Executors.newSingleThreadExecutor().submit( () -> {
            long targetEvent = firstEventNumber;
            initialisingFuture.complete(HBaseEventSubscription.this);
            while (!closing) {
                try {
                    Event evt = waitForEvent(targetEvent);
                    dispatcher.dispatch(evt);
                    targetEvent++;
                } catch (InterruptedException exp ) {
                    closing = true;
                } catch (ClosedByInterruptException exp ) {
                    closing = true;
                } catch (Exception exp ) {
                    logger.error("Error in event reader - closing subscription",exp);
                    closing = true;
                }
            }
            logger.info("Subscription closed for channel "+ table.getName());
        });

    }

    @Override
    public void close()  {
        reader.cancel(true);
        // drain and stop the dispatcher.
        dispatcher.stop();
    }

    // This will sleep only the reading thread
    // originally did this with a java.nio.WatchService but it was more complex and
    // did not allow fine grain control of the watchWindow.
    private final int WATCH_WINDOW_MILLIS = 3;
    private Event waitForEvent(final long eventNumber) throws Exception {

        logger.debug("Waiting for event " + eventNumber);
        Get getter = new Get(Bytes.toBytes( eventNumber ));

        Event event = null;
        while ( event == null ) {
            Result r = channelTable.get(getter);
            if ( r.isEmpty() ) {
                Thread.sleep( WATCH_WINDOW_MILLIS);
            } else {
                final long timeStamp = r.rawCells()[0].getTimestamp();
                final byte[] value = r.getValue(HBaseEventSink.colFamily, HBaseEventSink.qualifier);
                event = new FileEvent(eventNumber,timeStamp,0L, Unpooled.wrappedBuffer(value));
            }
        }
        logger.debug("Got Event " + eventNumber);
        return event;
    }

}