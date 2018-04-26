package example.gettingstarted.checkoutkata;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class PricingEngineImpl {

    private final Map<String, Integer> prices;

    public PricingEngineImpl(Map<String, Integer> prices) {
        this.prices = prices;
    }

    Integer price(Map<String, Integer> products) {
        Optional<BigDecimal> total = Optional.of(BigDecimal.ZERO);

        return products.entrySet().stream().mapToInt(productQuantity -> {
            final String product = productQuantity.getKey();
            final Integer quantity = productQuantity.getValue();
            final Optional<Integer> optPrice = Optional.ofNullable(prices.get(product));

            return optPrice.map(price -> price * quantity).orElse(0);
        }).sum();
    }

}
