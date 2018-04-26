package example.gettingstarted.checkoutkata;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSource;
import io.mewbase.projection.ProjectionManager;

import java.util.Map;
import java.util.stream.Collectors;

public class Main {

    private static final Map<String, Integer> prices;
    private static final PricingEngineImpl pricingEngine;

    static {
        prices = Maps.newHashMap();
        prices.put("banana", 80);
        prices.put("bread", 100);
        prices.put("crisps", 60);

        pricingEngine = new PricingEngineImpl(prices);
    }

    private static Map<String, Integer> basketFromDocument(BsonObject document) {
        return document
                .keyStream()
                .filter(key -> !key.equalsIgnoreCase("total"))
                .collect(Collectors.toMap(key -> key, document::getInteger));
    }

    public static void main(String[] args) throws Exception {
        final Config config = ConfigFactory.load("example/gettingstarted/checkoutkata/configuration.conf");

        final EventSource eventSource = EventSource.instance(config);
        final BinderStore binderStore = BinderStore.instance(config);
        final ProjectionManager projectionManager = ProjectionManager.instance(eventSource, binderStore);

        projectionManager.builder()
                .projecting("purchase_events")
                .identifiedBy(event -> event.getBson().getString("customer_id"))
                .filteredBy(event -> event.getBson().getString("action", "").equalsIgnoreCase("BUY"))
                .onto("customer_billing")
                .as((document, event) -> {
                    final BsonObject bson = event.getBson();

                    final String product = bson.getString("product");
                    final Integer quantity = bson.getInteger("quantity");

                    final Integer documentQuantity = document.getInteger(product, 0);

                    document.put(product, documentQuantity + quantity);

                    final Map<String, Integer> basket = basketFromDocument(document);

                    final Integer total = pricingEngine.price(basket);

                    document.put("total", total);

                    return document;
                })
                .named("customer_billing")
                .create();
    }

}
