package example.gettingstarted.projectionrest;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.BinderStore;
import io.mewbase.rest.DocumentLookup;
import io.mewbase.rest.RestServiceAdaptor;

public class Main {

    public static void main(String[] args) {
        String resourceBasename = "example.gettingstarted.projectionrest/configuration.conf";
        final Config config = ConfigFactory.load(resourceBasename);

        final RestServiceAdaptor restServiceAdaptor = RestServiceAdaptor.instance(config);
        final BinderStore binderStore = BinderStore.instance(config);

        /*
        Expose endpoint to retrieve a document from the binder store
         */
        restServiceAdaptor.exposeGetDocument(binderStore, "/summary/:product/:date", incomingRequest -> {
            //product and date form the document index in the binder
            final String product = incomingRequest.getPathParameters().get("product");
            final String date = incomingRequest.getPathParameters().get("date");

            //document is always in the sales_summary binder, and has a key built from incoming request parameters
           return new DocumentLookup("sales_summary", product + "_" + date);
        });

        restServiceAdaptor.start();
    }
}
