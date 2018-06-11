package io.mewbase.eventsource;


/**
 * Any Subscription to an EventSource needs an instance of an EventHandler.
 *
 * The handler is called when an event arrives from the source of the event.
 *
 * The handler would normally be expected to perform a very lightweight operation or
 * to post the event to a reactive stream for example that processes the event on another thread(s).
 * I.e. the EventHandler should not block or hold the event dispatcher thread(s) for undue periods.
 *
 * EventHandler is functional interface hence from Java 8 the EventHandler can be defined using
 * lambda expression ```(argument) -> (body)``` syntax in place.
 */
@FunctionalInterface
public interface EventHandler {

    void onEvent(Event evt);

}
