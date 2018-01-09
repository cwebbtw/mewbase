package io.mewbase.cqrs;


import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSource;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


import static junit.framework.TestCase.assertNull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Created by Nige on 16/10/17.
 */
@RunWith(VertxUnitRunner.class)
public class CommandTest extends MewbaseTestBase {

    final EventSink TEST_EVENT_SINK = EventSink.instance();

    final String COMMAND_NAME = "TestCommand";
    final String CHANNEL_NAME = "CommandTestChannel";

    final String COMMAND_INPUT_KEY = "InputKey";
    final String EVENT_OUTPUT_KEY = "OutputKey";

    final String INPUT_VALUE = "input";
    final String POST_SCRIPT = "+output";
    final String OUTPUT_VALUE = INPUT_VALUE + POST_SCRIPT;


    @Test
    public void testCommandManager() {

        CommandManager mgr = CommandManager.instance(TEST_EVENT_SINK);
        assertNotNull(mgr);
        final String COMMAND_NAME = "NotACommand";
        assertNotNull(mgr.commandBuilder());
        assertEquals(0, mgr.getCommands().count()); // no commands registered
        CompletableFuture<BsonObject> futEvt = mgr.execute(COMMAND_NAME, new BsonObject() );
        futEvt.handle( (good, bad) -> {
           assertNull("Executing a non command should fail.", good);
           assertNotNull("Executing a non command should not result in an event", bad);
           final String msg = bad.getMessage();
           assertTrue(msg.contains(COMMAND_NAME));
           return null;
        });
    }


    @Test
    public void testCommandOK() throws Exception {

        CommandManager mgr = CommandManager.instance(TEST_EVENT_SINK);

        Function<BsonObject, BsonObject> handler = (params) -> {
            assertNotNull( "Params should be non null", params);
            final String cmdInput = params.getString(COMMAND_INPUT_KEY);
            final BsonObject event = new BsonObject();
            event.put(EVENT_OUTPUT_KEY, cmdInput + POST_SCRIPT);
            return event;
        };

        CommandBuilder cBuilder = mgr.commandBuilder();
        Command cmd = cBuilder.named(COMMAND_NAME)
                        .emittingTo(CHANNEL_NAME)
                        .as(handler)
                        .create();

        // Command construction and getters.
        assertNotNull(cmd);
        assertEquals(COMMAND_NAME, cmd.getName());
        assertEquals(CHANNEL_NAME, cmd.getOutputChannel());
        assertEquals(handler, cmd.getFunction());

        //Subscribe to the EventSource
        CountDownLatch cdl = new CountDownLatch(1);
        EventSource evtSrc = new NatsEventSource();
        EventHandler evtHandler = evt -> {
            BsonObject event = evt.getBson();
            assertEquals(OUTPUT_VALUE, event.getString(EVENT_OUTPUT_KEY));
            cdl.countDown();
        };
        evtSrc.subscribe(CHANNEL_NAME,evtHandler);

        // Execute the command
        BsonObject params = new BsonObject().put(COMMAND_INPUT_KEY, INPUT_VALUE);
        BsonObject event = mgr.execute(COMMAND_NAME, params).join();
        assertEquals(OUTPUT_VALUE, event.getString(EVENT_OUTPUT_KEY));

        // wait for the evtHandler to receive the event and check that the event was placed on
        // the correct channel and was in hte correct transformed state.
        cdl.await();
    }


    @Test
    public void testCommandFail() throws Exception {

        CommandManager mgr = CommandManager.instance(TEST_EVENT_SINK);

        Function<BsonObject, BsonObject> handler = (params) -> {
            String empty = params.getString("NoExistentKey");
            empty.length();     // **** provokes an NPE ****
            return new BsonObject();
        };

        CommandBuilder cBuilder = mgr.commandBuilder();
        Command cmd = cBuilder.named(COMMAND_NAME)
                .emittingTo(CHANNEL_NAME)
                .as(handler)
                .create();

        BsonObject params = new BsonObject().put(COMMAND_INPUT_KEY, INPUT_VALUE);

        CountDownLatch cdl = new CountDownLatch(1);
        mgr.execute(COMMAND_NAME, params).handle( (worked, exp) -> {
            assertNull("Command throwing exception succeeded",worked);
            assertNotNull(exp);
            cdl.countDown();
            return null;
        } );

        cdl.await();
    }



    @Test
    public void testMultipleCommands(TestContext testContext) throws Exception {

        CommandManager mgr = CommandManager.instance(TEST_EVENT_SINK);
        CommandBuilder builder = mgr.commandBuilder();

        Stream<String> names = IntStream.rangeClosed(1,16).mapToObj( (index) -> {
            final String commandName = COMMAND_NAME + index;
            Command cmd = builder.emittingTo(CHANNEL_NAME)
                .named(commandName)
                .as((params) -> {
                    assertNotNull("Params should be non null", params);
                    final String cmdInput = params.getString(COMMAND_INPUT_KEY);
                    final BsonObject event = new BsonObject();
                    event.put(EVENT_OUTPUT_KEY, cmdInput + POST_SCRIPT);
                    return event;
                    })
                .create();
            return commandName;
        } );

        // check that all of the commands names match
        Set<String> generatedNames = names.collect(Collectors.toSet());
        Set<String> storedNames = mgr.getCommands().map( cmd -> cmd.getName()).collect(Collectors.toSet());

        // assert they are exactly the same
        assertTrue(generatedNames.containsAll(storedNames));
        assertTrue(storedNames.containsAll(generatedNames));

        BsonObject params = new BsonObject();
        Stream<CompletableFuture<BsonObject>> futs = generatedNames.stream().map(name ->
            mgr.execute(name, params.put(COMMAND_INPUT_KEY,name))
        );

        // check each future executed and returned a result.
        futs.forEach(fut -> fut.thenAccept( event -> assertNotNull(event)));
    }
}

