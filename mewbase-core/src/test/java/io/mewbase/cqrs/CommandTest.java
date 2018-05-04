package io.mewbase.cqrs;


import com.typesafe.config.Config;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;

import io.mewbase.eventsource.Subscription;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

    final String COMMAND_NAME = "TestCommand";


    final String COMMAND_INPUT_KEY = "InputKey";
    final String EVENT_OUTPUT_KEY = "OutputKey";

    final String INPUT_VALUE = "input";
    final String POST_SCRIPT = "+output";
    final String OUTPUT_VALUE = INPUT_VALUE + POST_SCRIPT;


    @Test
    public void testCommandManager() throws Exception {

        final Config cfg = createConfig();
        final EventSink sink = EventSink.instance(cfg);
        final CommandManager mgr = CommandManager.instance(sink);

        assertNotNull(mgr);
        final String COMMAND_NAME = "NotACommand";
        assertNotNull(mgr.commandBuilder());
        assertEquals(0, mgr.getCommands().count()); // no commands registered
        CompletableFuture<Long> futEvt = mgr.execute(COMMAND_NAME, new BsonObject() );
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

        final Config cfg = createConfig();
        final EventSink sink = EventSink.instance(cfg);
        final EventSource source  = EventSource.instance(cfg);
        final CommandManager mgr = CommandManager.instance(sink);

        final String TEST_CHANNEL_NAME =  "CommandTestChannel"+ UUID.randomUUID();

        Function<BsonObject, BsonObject> handler = (params) -> {
            assertNotNull( "Params should be non null", params);
            final String cmdInput = params.getString(COMMAND_INPUT_KEY);
            final BsonObject event = new BsonObject();
            event.put(EVENT_OUTPUT_KEY, cmdInput + POST_SCRIPT);
            return event;
        };

        CommandBuilder cBuilder = mgr.commandBuilder();
        Command cmd = cBuilder.named(COMMAND_NAME)
                        .emittingTo(TEST_CHANNEL_NAME)
                        .as(handler)
                        .create();

        // Command construction and getters.
        assertNotNull(cmd);
        assertEquals(COMMAND_NAME, cmd.getName());
        assertEquals(TEST_CHANNEL_NAME, cmd.getOutputChannel());
        assertEquals(handler, cmd.getFunction());

        //Subscribe to the EventSource
        CountDownLatch cdl = new CountDownLatch(1);

        EventHandler evtHandler = evt -> {
            BsonObject event = evt.getBson();
            assertEquals(OUTPUT_VALUE, event.getString(EVENT_OUTPUT_KEY));
            cdl.countDown();
        };
        Future<Subscription> subsFut = source.subscribe(TEST_CHANNEL_NAME,evtHandler);
        subsFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        // Execute the command
        BsonObject params = new BsonObject().put(COMMAND_INPUT_KEY, INPUT_VALUE);
        mgr.execute(COMMAND_NAME, params).join();


        // wait for the evtHandler to receive the event and check that the event was placed on
        // the correct channel and was in the correct transformed state.
        cdl.await();
    }


    @Test
    public void testCommandFail() throws Exception {

        final Config cfg =  createConfig();
        final EventSink sink = EventSink.instance(cfg);
        final CommandManager mgr = CommandManager.instance(sink);

        final String TEST_CHANNEL_NAME =  "CommandTestChannel"+ UUID.randomUUID();

        Function<BsonObject, BsonObject> handler = (params) -> {
            String empty = params.getString("NoExistentKey");
            empty.length();     // **** provokes an NPE ****
            return new BsonObject();
        };

        CommandBuilder cBuilder = mgr.commandBuilder();
        Command cmd = cBuilder.named(COMMAND_NAME)
                .emittingTo(TEST_CHANNEL_NAME)
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

        final Config cfg = createConfig();
        final EventSink sink = EventSink.instance(cfg);
        final CommandManager mgr = CommandManager.instance(sink);
        final CommandBuilder builder = mgr.commandBuilder();

        final String TEST_CHANNEL_NAME =  "CommandTestChannel"+ UUID.randomUUID();

        final Stream<String> names = IntStream.rangeClosed(1,16).mapToObj( (index) -> {
            final String commandName = COMMAND_NAME + index;
            Command cmd = builder.emittingTo(TEST_CHANNEL_NAME)
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

        final BsonObject params = new BsonObject();
        Stream<CompletableFuture<Long>> futs = generatedNames.stream().map(name ->
            mgr.execute(name, params.put(COMMAND_INPUT_KEY,name))
        );

        // check each future executed and returned a result.
        futs.forEach(fut -> fut.thenAccept( event -> assertNotNull(event)));
    }
}

