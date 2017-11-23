package io.mewbase.example.sink;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import io.mewbase.eventsource.impl.nats.NatsEventSink;

import java.time.Instant;

/**
 *
 * Run this example to generate timestamped events until it is cancelled by an OS signal
 * You can set the rate or events (per second) and event channel where given.
 * java -cp  <libraries>/mewbase-<version>-jar-with-dependencies.jar
 *                  io.mewbase.example.sink.SinkExample
 *
 */
public class SinkExample {

    public static void main(String[] args) throws Exception {

        EventSink eventSink = new NatsEventSink();

        // Change these for channels and rate changes
        final String CHANNEL_NAME  = "TestChannel";
        final Float eventsPerSecond = 1.0f;

        // These are required
        final int milliSecondWait = Math.max(1,(int)(1000.0f / eventsPerSecond));
        final BsonObject event = new BsonObject();

        while (!Thread.interrupted() ) {
            final String timeStamp = "Time :" + Instant.now();
            event.put("timestamp", timeStamp);      // overwrite previous value
            eventSink.publish(CHANNEL_NAME,event);  // publish the event to the sink on the given channel
            Thread.sleep( milliSecondWait );
        }
        eventSink.close();
    }

}
