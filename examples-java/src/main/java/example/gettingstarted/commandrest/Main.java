package example.gettingstarted.commandrest;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Command;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.eventsource.EventSink;
import io.mewbase.rest.RestServiceAdaptor;

import java.time.Instant;

public class Main {

    public static void main(String[] args) {
        final Config config = ConfigFactory.load("example.gettingstarted.commandrest/configuration.conf");

        final EventSink eventSink = EventSink.instance(config);
        final RestServiceAdaptor restServiceAdaptor = RestServiceAdaptor.instance(config);
        final CommandManager commandManager = CommandManager.instance(eventSink);

        // command which emits a buy purchase event
        final Command buyCommand =
                commandManager
                        .commandBuilder()
                        .named("buy")
                        .as(params -> {
                            final BsonObject event = new BsonObject();
                            event.put("product", params.getBsonObject("body").getString("product")); // product copied from incoming HTTP post
                            event.put("quantity", params.getBsonObject("body").getInteger("quantity")); // quantity copied from incoming HTTP post
                            event.put("action", "BUY"); // action is always BUY
                            return event;
                        })
                        .emittingTo("purchase_events") // Kafka topic to emit event on
                        .create();

        // command which emits a refund purchase event
        final Command refundCommand =
                commandManager
                        .commandBuilder()
                        .named("refund")
                        .as(params -> {
                            final BsonObject event = new BsonObject();
                            event.put("product", params.getBsonObject("body").getString("product")); // product copied from incoming HTTP post
                            event.put("quantity", params.getBsonObject("body").getInteger("quantity")); // quantity copied from incoming HTTP post
                            event.put("action", "REFUND"); // action is always REFUND
                            return event;
                        })
                        .emittingTo("purchase_events") // Kafka topic to emit event on
                        .create();

        // expose the buy and refund commands
        restServiceAdaptor.exposeCommand(commandManager, buyCommand.getName());
        restServiceAdaptor.exposeCommand(commandManager, refundCommand.getName());

        restServiceAdaptor.start();
    }

}
