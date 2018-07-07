package io.mewbase.rest.vertx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.rest.RestServiceAction;
import io.mewbase.rest.RestServiceAdaptor;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class VertxRestServiceAdapter implements RestServiceAdaptor {

    private final static Logger logger = LoggerFactory.getLogger(VertxRestServiceAdapter.class);

    final ObjectMapper mapper = new ObjectMapper();

    private static final String binderParamKey = "binderName";
    private static final String documentParamKey = "documentId";
    private static final String binderParam = "/:" + binderParamKey;
    private static final String documentParam = "/:" + documentParamKey;

    private final HttpServer httpServer;
    private final Router router;

    public VertxRestServiceAdapter() {
        this(ConfigFactory.load());
    }

    public VertxRestServiceAdapter(Config cfg) {
        // Get the Vertx rest end point configuration
        final String host = cfg.getString("mewbase.api.rest.vertx.host");
        final int port = cfg.getInt("mewbase.api.rest.vertx.port");
        final Duration timeout = cfg.getDuration("mewbase.api.rest.vertx.timeout");
        final HttpServerOptions opts = new HttpServerOptions().setHost(host).setPort(port);

        // Set up the rest server using the config.
        final Vertx vertx = Vertx.vertx();
        httpServer = vertx.createHttpServer(opts);
        router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        httpServer.requestHandler(router::accept);
        logger.info("Created Rest Adapter on "+ opts.getHost() + ":" + opts.getPort() );
    }

    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore) {
        return exposeGetDocument(binderStore, "/binders");
    }

    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String uriPathPrefix) {
        router.route(HttpMethod.GET, uriPathPrefix).handler(routingContext -> {
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            actionVisitor.visit(RestServiceAction.listBinders(binderStore));
        });
        router.route(HttpMethod.GET, uriPathPrefix + binderParam).handler(routingContext -> {
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            final MultiMap params = routingContext.request().params();
            final String binderName = params.get(binderParamKey);
            actionVisitor.visit(RestServiceAction.listDocumentIds(binderStore, binderName));
        });
        router.route(HttpMethod.GET, uriPathPrefix + binderParam + documentParam).handler(routingContext -> {
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            final MultiMap params = routingContext.request().params();
            final String binderName = params.get(binderParamKey);
            final String documentId = params.get(documentParamKey);
            actionVisitor.visit(RestServiceAction.retrieveSingleDocument(binderStore, binderName, documentId));
        });
        return this;
    }

    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String binderName, String uriPathPrefix) {
        final String binderNameWithSlashPrefix = binderName.startsWith("/") ? binderName : "/" + binderName;

        router.route(HttpMethod.GET, uriPathPrefix + binderNameWithSlashPrefix).handler(routingContext -> {
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            actionVisitor.visit(RestServiceAction.listDocumentIds(binderStore, binderName));
        });

        router.route(HttpMethod.GET, uriPathPrefix + binderNameWithSlashPrefix + documentParam).handler(routingContext -> {
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            final MultiMap params = routingContext.request().params();
            final String documentId = params.get(documentParamKey);
            actionVisitor.visit(RestServiceAction.retrieveSingleDocument(binderStore, binderName, documentId));
        });

        return this;
    }

    @Override
    public RestServiceAdaptor exposeCommand(CommandManager commandManager, String commandName) {
        return exposeCommand(commandManager, commandName, "/");
    }

    @Override
    public RestServiceAdaptor exposeCommand(CommandManager commandManager, String commandName, String uriPathPrefix) {
        final String commandNameWithSlashPrefix = uriPathPrefix.endsWith("/") ? commandName : "/" + commandName;

        router.route(HttpMethod.POST, uriPathPrefix + commandNameWithSlashPrefix).handler(routingContext -> {
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            final BsonObject body = actionVisitor.bodyAsBson();
            actionVisitor.visit(RestServiceAction.executeCommand(commandManager, commandName, body));
        });
        return this;
    }

    @Override
    public RestServiceAdaptor exposeQuery(QueryManager queryManager, String queryName) {
        return exposeQuery(queryManager, queryName, "/");
    }

    @Override
    public RestServiceAdaptor exposeQuery(QueryManager queryManager, String queryName, String uriPathPrefix) {
        final String queryNameWithSlashPrefix = uriPathPrefix.endsWith("/") ? queryName : "/" + queryName;

        router.route(HttpMethod.GET, uriPathPrefix + queryNameWithSlashPrefix).handler(routingContext -> {
            final VertxRestServiceActionVisitor actionVisitor = new VertxRestServiceActionVisitor(routingContext);
            actionVisitor.visit(RestServiceAction.runQuery(queryManager, queryName, new BsonObject()));
        });
        return this;
    }

    public CompletableFuture<Void> start() {
        AsyncResCF<HttpServer> ar = new AsyncResCF<>();
        logger.info("Starting Rest Adapter");
        httpServer.listen(ar);
        return ar.thenApply(server -> null);
    }

    @Override
    public int getServerPort() {
        return httpServer.actualPort();
    }

    public CompletableFuture<Void> stop() {
        AsyncResCF<Void> ar = new AsyncResCF<>();
        logger.info("Stopping Rest Adapter");
        httpServer.close(ar);
        return ar;
    }

    @Override
    public void close() {
        try {
            stop().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
