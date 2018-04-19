package example.gettingstarted.commandrest;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.Command;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.eventsource.EventSink;
import io.mewbase.rest.RestServiceAdaptor;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        final Config config = ConfigFactory.load("example/gettingstarted/commandrest/configuration.conf");

        try (final EventSink eventSink = EventSink.instance(config)) {
            try (final RestServiceAdaptor restServiceAdaptor = RestServiceAdaptor.instance(config)) {
                final CommandManager commandManager = CommandManager.instance(eventSink);

                final Command buyCommand =
                        commandManager
                            .commandBuilder()
                            .named("buy")
                            .as(params -> {
                                final BsonObject event = new BsonObject();
                                event.put("product", params.getBsonObject("body").getString("product"));
                                event.put("quantity", params.getBsonObject("body").getInteger("quantity"));
                                event.put("action", "BUY");
                                return event;
                            })
                            .emittingTo("purchase_events")
                            .create();

                final Command refundCommand =
                        commandManager
                                .commandBuilder()
                                .named("refund")
                                .as(params -> {
                                    final BsonObject event = new BsonObject();
                                    event.put("product", params.getBsonObject("body").getString("product"));
                                    event.put("quantity", params.getBsonObject("body").getInteger("quantity"));
                                    event.put("action", "REFUND");
                                    return event;
                                })
                                .emittingTo("purchase_events")
                                .create();

                restServiceAdaptor.exposeCommand(commandManager, buyCommand.getName());
                restServiceAdaptor.exposeCommand(commandManager, refundCommand.getName());

                restServiceAdaptor.start();
                try {
                    System.in.read();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
