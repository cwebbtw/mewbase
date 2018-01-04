package io.mewbase.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import com.typesafe.config.ConfigFactory;
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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import java.util.stream.Stream;

/**
 * Created by Tim on 11/01/17.
 */
public class VertxRestServiceAdaptor implements RestServiceAdaptor {

    private final static Logger logger = LoggerFactory.getLogger(VertxRestServiceAdaptor.class);

    final ObjectMapper mapper = new ObjectMapper();

    private static final String binderParamKey = "binderName";
    private static final String documentParamKey = "documentId";
    private static final String binderParam = "/:" + binderParamKey;
    private static final String documentParam = "/:" + documentParamKey;


    private final HttpServer httpServer;
    private final Router router;


    public VertxRestServiceAdaptor() {
        this(ConfigFactory.load());
    }


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
    public RestServiceAdaptor exposeGetDocument(final BinderStore binderStore) {
        return exposeGetDocument(binderStore, "/binders");
    }


    @Override
    public RestServiceAdaptor exposeGetDocument(final BinderStore binderStore, String uriPathPrefix) {

        // list all of the binders
        router.route(HttpMethod.GET, uriPathPrefix).handler(rc -> {
            final HttpServerResponse response = rc.response();
            response.
                    putHeader("content-type", "application/json").
                    end(convertToJson(binderStore.binderNames().toArray()));
        } );

        // list the document Ids in a binder.
        final String binderQualifingUri = uriPathPrefix + binderParam;
        router.route(HttpMethod.GET, binderQualifingUri).handler(rc -> {
            final MultiMap params = rc.request().params();
            final String binderName = params.get(binderParamKey);

            final HttpServerResponse response = rc.response();
            // Todo - needs getDocumentIds on Binder rather than as map over the kv pairs
            final Stream ids = binderStore.
                                    open(binderName).
                                    getDocuments().
                                    map( kv -> kv.getKey() );
            response.
                    putHeader("content-type", "application/json").
                    end(convertToJson(ids.toArray()));

        } );

        // get the given document from docID or not.
        final String documentQualifingUri = binderQualifingUri + documentParam;
        router.route(HttpMethod.GET, documentQualifingUri).handler(rc -> {
            final MultiMap params = rc.request().params();
            final String binderName = params.get(binderParamKey);
            final String documentId = params.get(documentParamKey);

            final HttpServerResponse response = rc.response();
            binderStore.open(binderName).get(documentId).whenComplete((doc, thbl) -> {
                if (thbl != null ) {
                    String errMsg = "Error whilst trying to find " + documentId + " in binder " + binderName;
                    logger.error(errMsg, thbl);
                    response.setStatusCode(500).setStatusMessage(errMsg).end();
                } else if (doc == null) {
                    String failMsg = "Failed to find "+ documentId +" in binder "+ binderName;
                    response.setStatusCode(500).setStatusMessage(failMsg).end();
                } else {
                    response.
                            putHeader("content-type","application/json").
                            end(doc.encodeToString());
                }
            });
        } );
        return this;
    }


    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String binderName, String uriPathPrefix) {
        // list the document Ids in a binder.
        final String binderQualifingUri = uriPathPrefix + binderName;
        router.route(HttpMethod.GET, binderQualifingUri).handler(rc -> {
            final HttpServerResponse response = rc.response();
            // Todo needs getDocumentIds on Binder
            final Stream ids = binderStore.
                    open(binderName).
                    getDocuments().
                    map( kv -> kv.getKey() );
            response.
                    putHeader("content-type", "application/json").
                    end(convertToJson(binderStore.binderNames().toArray()));
        } );


        // get the given document from docID or not.
        final String documentQualifingUri = binderQualifingUri + documentParam;
        router.route(HttpMethod.GET, documentQualifingUri).handler(rc -> {
            final MultiMap params = rc.request().params();
            final String documentId = params.get(binderParamKey);

            final HttpServerResponse response = rc.response();
            binderStore.open(binderName).get(documentId).whenComplete( (doc, t) -> {
                if (t != null) {
                    String errMsg = "Failed to find "+ documentId +" in binder "+ binderName;
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
        } );
        return this;
    }


    @Override
    public RestServiceAdaptor exposeCommand(final CommandManager commandMgr, String commandName) {
        return exposeCommand(commandMgr, commandName, "");
    }


    @Override
    public RestServiceAdaptor exposeCommand(final CommandManager commandMgr, String commandName, String uriPathPrefix) {
        // Todo - Check that command manager has the named command
        // the command url is any prefix plus the command name
        final String uri = uriPathPrefix + "/" + commandName;
        router.route(HttpMethod.POST, uri).handler(rc -> {
            rc.setAcceptableContentType("application/json");
            // Pack the path params and body into the call context
            final BsonObject context = new BsonObject();
            final Map pathParams = rc.pathParams();
            context.put("pathParams", new BsonObject(pathParams));
            final JsonObject body = rc.getBodyAsJson();
            context.put("body", new BsonObject(body));
            // Dispatch the command in context on another thread
            CompletableFuture<BsonObject> cf = commandMgr.execute(commandName, context);
            rc.response().setStatusCode(200).end();
        });
        return this;
    }



    @Override
    public RestServiceAdaptor exposeQuery(final QueryManager qmgr, String queryName) {
        return exposeQuery(qmgr, queryName, "");
    }

    @Override
    public RestServiceAdaptor exposeQuery(final QueryManager qmgr, String queryName, String uriPathPrefix) {

            final String uri = uriPathPrefix + "/" + queryName;
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


    /**
     * Helper to trap the Exception side effect on a failed conversion to Json
     * @return
     */
    private final String convertToJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception exp) {
            final String errMsg = "Failed to convert Java object " + obj + " to Json";
            logger.error(errMsg,exp);
            return errMsg;
        }
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
