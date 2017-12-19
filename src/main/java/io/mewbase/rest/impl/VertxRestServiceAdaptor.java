package io.mewbase.rest.impl;

import com.typesafe.config.Config;

import io.mewbase.binders.BinderStore;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.rest.RestServiceAdaptor;
import io.mewbase.util.AsyncResCF;



import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import java.util.stream.Stream;

/**
 * Created by tim on 11/01/17.
 */
public class VertxRestServiceAdaptor implements RestServiceAdaptor {

    private final static Logger logger = LoggerFactory.getLogger(VertxRestServiceAdaptor.class);

    private final HttpServer httpServer;
    private final Router router;


    public VertxRestServiceAdaptor(Config cfg) {
        // Get the Vertx rest end point configuration
        final String host = cfg.getString("mewbase.api.rest.vertex.host");
        final int port = cfg.getInt("mewbase.api.rest.vertex.port");
        final Duration timeout = cfg.getDuration("mewbase.api.rest.vertex.timeout");
        final HttpServerOptions opts = new HttpServerOptions().setHost(host).setPort(port);

        // Set up the rest server using the config.
        final Vertx vertx = Vertx.vertx();
        httpServer = vertx.createHttpServer(opts);
        router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        httpServer.requestHandler(router::accept);
    }


    @Override
    public RestServiceAdaptor exposeCommand(CommandManager commandMgr, String commandName, String uri) {

        router.route(HttpMethod.POST, uri).handler(rc -> {
            rc.setAcceptableContentType("application/json");

            // Pack the path params and body into the call context
            BsonObject context = new BsonObject();
            context.put("pathParams", new BsonObject(rc.pathParams()));
            context.put("body", new BsonObject(rc.getBody()));
            // Dispatch the command in context on another thread
            CompletableFuture<BsonObject> cf = commandMgr.execute(commandName, context);
            rc.response().setStatusCode(200).end();
        });
        return this;
    }


    @Override
    public RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName, String uri) {

       router.route(HttpMethod.GET, uri).handler(rc -> {
           // dispatch the query
           rc.setAcceptableContentType("application/json");
           BsonObject context = new BsonObject();
           context.put("pathParams", new BsonObject(rc.pathParams()));
           Stream<KeyVal<String, BsonObject>> binders = qmgr.execute(queryName,context);

           // assemble the response
           BsonObject payload = new BsonObject();
           binders.map( kv -> payload.put(kv.getKey(),kv.getValue()));

           rc.response().
               putHeader("content-type","application/json").
               end(payload.encodeToString());
        });
       return this;
    }


    @Override
    public RestServiceAdaptor exposeFindByID(BinderStore binderStore, String uri) {

        router.route(HttpMethod.GET, uri).handler(rc -> {
            MultiMap params = rc.request().params();
            String binderName = params.get("binder");
            String id = params.get("id");

            HttpServerResponse response = rc.response();
            if (binderName == null || id == null) {
                    response.
                        setStatusCode(404).
                        setStatusMessage("Missing Parameter for 'binder' and/or 'id'.").
                        end();
            } else {
                binderStore.open(binderName).get(id).whenComplete((doc, t) -> {
                    if (t != null) {
                        String errMsg = "Failed to find "+ id +" in binder "+ binderName;
                        logger.error(errMsg, t);
                        response.
                            setStatusCode(500).
                            setStatusMessage(errMsg).
                            end();
                    } else {
                        response.
                            putHeader("content-type","application/json").
                            end(doc.encodeToString());
                    }
                });
            }
        });
        return this;
    }


    public CompletableFuture<Void> start() {
        AsyncResCF<HttpServer> ar = new AsyncResCF<>();
        httpServer.listen(ar);
        return ar.thenApply(server -> null);
    }

    public CompletableFuture<Void> stop() {
        AsyncResCF<Void> ar = new AsyncResCF<>();
        httpServer.close(ar);
        return ar;
    }


}
