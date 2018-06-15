package io.mewbase.eventsource.impl;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.Arrays;
import java.util.List;


/**
 * Shims are used for two main purposes ...
 *
 * 1) Instrument the Calls to the underlying class
 * 2) Implement security on a call by call basis possibly wrt Counters and other instrumentation
 */

public class EventHandlerShim implements EventHandler {

    private static final String METRICS_NAME = "mewbase.event.source.event";

    private final Counter eventCounter;

    private final EventHandler impl;

    public EventHandlerShim(String channelName, EventHandler impl) {
        List<Tag> tag = Arrays.asList(Tag.of("channel", channelName));
        // This results in a call down to the meter registry which will either register the counter or
        // return the one currently in scope for this id/tag combination.
        // From the Micrometer MeterRegistry java doc ...
        // Add the counter to a single registry, or return an existing counter in that registry. The returned
        // counter will be unique for each registry, but each registry is guaranteed to only create one counter
        // for the same combination of name and tags.
        eventCounter = Metrics.counter( METRICS_NAME, tag );
        this.impl = impl;
    }


    @Override
    public void onEvent(Event evt) {
        eventCounter.increment();
        impl.onEvent(evt);
    }

}
