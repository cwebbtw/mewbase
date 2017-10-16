package io.mewbase.cqrs;

import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.impl.CommandManagerImpl;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        assertNotNull(mgr.commandBuilder());
        assertEquals(0, mgr.getCommands().count()); // no commands registered
        CompletableFuture<BsonObject> futEvt = mgr.execute("NotACommand", new BsonObject() );
         // Todo - check futEvt completes exceptionally;
    }


    //@Test
    public void testCommandOK() throws Exception {

        String commandName = "testcommand";

//        CommandHandler handler = server.buildCommandHandler(commandName)
////                .emittingTo(TEST_CHANNEL_1)
////                .as((command, context) -> {
////                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
////                    context.complete();
////                })
//                .create();
//
//        assertNotNull(handler);
//        assertEquals(commandName, handler.getName());
//
//        Async async = testContext.async();

//        Consumer<ClientDelivery> subHandler = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals("foobar", event.getString("eventField"));
//            async.complete();
//        };

       // client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1), subHandler).get();
       // BsonObject sentCommand = new BsonObject().put("commandField", "foobar");
       // client.sendCommand(commandName, sentCommand).get();

    }


    @Test
    public void testCommandFail() throws Exception {

        String commandName = "testcommand";

//        CommandHandler handler = server.buildCommandHandler(commandName)
//                .emittingTo(TEST_CHANNEL_1)
//                .as((command, context) -> {
//                    context.completeExceptionally(new Exception("rejected"));
//                })
//                .create();
//
//        assertNotNull(handler);
//        assertEquals(commandName, handler.getName());
//
//        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

//        try {
//            client.sendCommand(commandName, sentCommand).get();
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
//            MewException me = (MewException)e.getCause();
//            // OK
//            testContext.assertEquals("rejected", me.getMessage());
//            testContext.assertEquals(Client.ERR_COMMAND_NOT_PROCESSED, me.getErrorCode());
//        }

    }

    @Test
    public void testCommandHandlerThrowsException(TestContext testContext) throws Exception {

        String commandName = "testcommand";

//        CommandHandler handler = server.buildCommandHandler(commandName)
////                .emittingTo(TEST_CHANNEL_1)
////                .as((command, context) -> {
////                    //throw new Exception("oops!");
////                })
//                .create();
//
//        assertNotNull(handler);
//        assertEquals(commandName, handler.getName());
//
//        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

//        try {
//           // client.sendCommand(commandName, sentCommand).get();
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
//            //MewException me = (MewException)e.getCause();
//            // OK
//            //testContext.assertEquals("oops!", me.getMessage());
//            //testContext.assertEquals(Client.ERR_COMMAND_NOT_PROCESSED, me.getErrorCode());
//        }

    }

    @Test
    public void testNoSuchCommandhandler(TestContext testContext) throws Exception {

        String commandName = "nocommand";

        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

//        try {
//            //client.sendCommand(commandName, sentCommand).get();
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
            //MewException me = (MewException)e.getCause();
            // OK
            //testContext.assertEquals("No handler for nocommand", me.getMessage());
            //testContext.assertEquals(Client.ERR_COMMAND_NOT_PROCESSED, me.getErrorCode());
       // }

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
