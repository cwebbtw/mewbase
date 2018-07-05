package io.mewbase.rest.vertx;

import io.mewbase.binders.BinderStore;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.rest.DocumentLookup;
import io.mewbase.rest.IncomingRequest;
import io.mewbase.rest.RestServiceAdaptor;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class VertxRestServiceAdapter implements RestServiceAdaptor {

    private final Vertx vertx;

    public VertxRestServiceAdapter(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore) {
        return null;
    }

    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String uriPathPrefix) {
        return null;
    }

    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String uri, Function<IncomingRequest, DocumentLookup> requestToDocumentLookup) {
        return null;
    }

    @Override
    public RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String binderName, String uriPathPrefix) {
        return null;
    }

    @Override
    public RestServiceAdaptor exposeCommand(CommandManager qmgr, String commandName) {
        return null;
    }

    @Override
    public RestServiceAdaptor exposeCommand(CommandManager qmgr, String commandName, String uriPathPrefix) {
        return null;
    }

    @Override
    public RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName) {
        return null;
    }

    @Override
    public RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName, String uriPathPrefix) {
        return null;
    }

    public Router build() {
        return null;
    }

    @Override
    public CompletableFuture<Void> start() {
        return null;
    }

    @Override
    public CompletableFuture<Void> stop() {
        return null;
    }

    @Override
    public void close() {

    }
}
