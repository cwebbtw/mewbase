package example.gettingstarted.projectionpostgres;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.BinderStore;
import io.mewbase.eventsource.EventSource;
import io.mewbase.projection.ProjectionManager;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Main {

    private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE;


    public static void main(String[] args) throws Exception {
        String resourceBasename = "example/gettingstarted/projectionpostgres/configuration.conf";
        final Config config = ConfigFactory.load(resourceBasename);

        final EventSource eventSource = EventSource.instance(config);
        final BinderStore binderStore = BinderStore.instance(config);
        final ProjectionManager projectionManager = ProjectionManager.instance(eventSource, binderStore);

        projectionManager.builder()
                .projecting("purchase_events") // which kafka topic to stream events from
                .identifiedBy(event -> {
                    /*
                    An event to buy a banana on 2018-01-01 15:00:00 affects the
                    banana_2018-01-01 document
                     */
                    final String product = event.getBson().getString("product");
                    final Instant timestamp = event.getInstant();
                    final ZonedDateTime zonedDateTime =
                            ZonedDateTime.ofInstant(timestamp, ZoneOffset.UTC);

                    return product + "_" + dateFormatter.format(zonedDateTime);
                })
                .filteredBy(event -> {
                    /*
                    Only consider incoming events with an action of BUY or REFUND
                     */
                    final String action = event.getBson().getString("action", "");
                    return action.equalsIgnoreCase("BUY") ||
                            action.equalsIgnoreCase("REFUND");
                })
                .onto("sales_summary") // the binder to store documents maintained by the project in
                .named("sales_summary_projection") // the name of this projection
                .as((document, event) -> {
                    //grab current document state
                    Integer buys = document.getInteger("buys", 0);
                    Integer refunds = document.getInteger("refunds", 0);

                    //grab useful information from incoming event
                    final String action = event.getBson().getString("action");
                    final Integer quantity = event.getBson().getInteger("quantity");

                    // adjust buys/refunds for document based on this event
                    switch (action) {
                        case "BUY":
                            buys += quantity;
                            break;
                        case "REFUND":
                            refunds += quantity;
                            break;
                    }

                    // update the document based on the adjustment
                    document.put("buys", buys);
                    document.put("refunds", refunds);
                    document.put("total", buys - refunds);
                    return document;
                })
                .create();
    }

}
