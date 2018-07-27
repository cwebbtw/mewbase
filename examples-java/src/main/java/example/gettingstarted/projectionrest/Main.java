package example.gettingstarted.projectionrest;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.BinderStore;
import io.mewbase.rest.RestServiceAction;
import io.mewbase.rest.RestServiceAdaptor;
import io.mewbase.rest.vertx.VertxRestServiceActionVisitor;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

/***
 * This example shows how to use Mewbase with an existing Vertx Http Server.
 *
 * You have control over endpoint configuration,
 * and can easily use the VertxRestServiceActionVisitor to pass control to Mewbase to perform document retrieval
 */
public class Main {

    public static void main(String[] args) {
        String resourceBasename = "example.gettingstarted.projectionrest/configuration.conf";
        final Config config = ConfigFactory.load(resourceBasename);

        // create a Vertx web server
        final Vertx vertx = Vertx.vertx();
        final HttpServerOptions options = new HttpServerOptions().setPort(8080);
        final HttpServer httpServer = vertx.createHttpServer(options);
        final BinderStore binderStore = BinderStore.instance(config);
        final Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        httpServer.requestHandler(router::accept);

        /*
        Expose endpoint to retrieve a document from the binder store
         */
        router.route(HttpMethod.GET, "/summary/:product/:date").handler(routingContext -> {
            final String product = routingContext.pathParams().get("product");
            final String date = routingContext.pathParams().get("date");
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            actionVisitor.visit(RestServiceAction.retrieveSingleDocument(binderStore, "sales_summary", product + "_" + date));
        });

        httpServer.listen();
    }
}
