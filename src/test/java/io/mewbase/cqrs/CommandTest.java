package io.mewbase.cqrs;

import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.impl.CommandManagerImpl;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by Nige on 16/10/17.
 */
@RunWith(VertxUnitRunner.class)
public class CommandTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(CommandTest.class);

    private final EventSink TEST_EVENT_SINK = new NatsEventSink();


    @Test
    public void testCommandManager() {

        CommandManager mgr = new CommandManagerImpl(TEST_EVENT_SINK);
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

        final String COMMAND_NAME = "TestCommand";
        final String CHANNEL_NAME = "CommandTestChannel";

        final String COMMAND_INPUT_KEY = "InputKey";
        final String EVENT_OUTPUT_KEY = "OutputKey";

        final String INPUT_VALUE = "input";
        final String POST_SCRIPT = "+output";
        final String OUTPUT_VALUE = INPUT_VALUE + POST_SCRIPT;

        CommandManager mgr = new CommandManagerImpl(TEST_EVENT_SINK);

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

        // Execute the command and wait for the magic to happen
        BsonObject inputParams = new BsonObject().put(COMMAND_INPUT_KEY, INPUT_VALUE);
        BsonObject event = mgr.execute(COMMAND_NAME, inputParams).join();
        assertEquals(OUTPUT_VALUE, event.getString(EVENT_OUTPUT_KEY));

        // wait for the evtHandler to receive the event and check that the event was placed on
        // the correct channel and was in hte correct transformed state.
        cdl.await();

    }


    @Test
    public void testCommandFail() throws Exception {

        final String COMMAND_NAME = "TestCommand";
        final String CHANNEL_NAME = "CommandTestChannel";

        final String COMMAND_INPUT_KEY = "InputKey";

        final String INPUT_VALUE = "input";


        CommandManager mgr = new CommandManagerImpl(TEST_EVENT_SINK);

        Function<BsonObject, BsonObject> handler = (params) -> {
            String empty = params.getString("duffKey");
            empty.length();
            return new BsonObject();
        };

        CommandBuilder cBuilder = mgr.commandBuilder();
        Command cmd = cBuilder.named(COMMAND_NAME)
                .emittingTo(CHANNEL_NAME)
                .as(handler)
                .create();

        BsonObject inputParams = new BsonObject().put(COMMAND_INPUT_KEY, INPUT_VALUE);

        CountDownLatch cdl = new CountDownLatch(1);
        mgr.execute(COMMAND_NAME, inputParams).handle( (worked, exp) -> {
            assertNull("Command throwing exception succeeded",worked);
            assertNotNull(exp);
            cdl.countDown();
            return null;
        } );

        cdl.await();

    }



   // @Test
    public void testMultipleCommandHandlers(TestContext testContext) throws Exception {

        String commandName1 = "testcommand1";

//        CommandManager handler1 = server.buildCommandHandler(commandName1)
//                .emittingTo(TEST_CHANNEL_1)
//                .as((command, context) -> {
//                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
//                    context.complete();
//                })
//                .create();

//        assertNotNull(handler1);
//        assertEquals(commandName1, handler1.getName());

        String commandName2 = "testcommand2";

//        CommandManager handler2 = server.buildCommandHandler(commandName2)
//                .emittingTo(TEST_CHANNEL_2)
//                .as((command, context) -> {
//                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
//                    context.complete();
//                })
//                .create();

//        assertNotNull(handler2);
 //       assertEquals(commandName2, handler2.getName());

        Async async1 = testContext.async();

//        Consumer<ClientDelivery> subHandler1 = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals(commandName1, event.getString("eventField"));
//            async1.complete();
//        };

        //client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1), subHandler1).get();

        Async async2 = testContext.async();

//        Consumer<ClientDelivery> subHandler2 = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals(commandName2, event.getString("eventField"));
//            async2.complete();
//        };

       // client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_2), subHandler2).get();

        BsonObject sentCommand1 = new BsonObject().put("commandField", commandName1);
//        client.sendCommand(commandName1, sentCommand1).get();

        BsonObject sentCommand2 = new BsonObject().put("commandField", commandName2);
//        client.sendCommand(commandName2, sentCommand2).get();

    }

}
