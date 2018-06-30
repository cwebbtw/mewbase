package io.mewbase.rest.vertx;

import io.mewbase.bson.BsonObject;
import io.mewbase.rest.RestAdapter;
import io.mewbase.rest.RestServiceAction;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

public class VertxRestServiceActionVisitor implements RestServiceAction.Visitor<Void> {

    private final RoutingContext rc;

    public VertxRestServiceActionVisitor(RoutingContext rc) {
        this.rc = rc;
    }

    public BsonObject bodyAsBson() {
        return new BsonObject(rc.getBodyAsJson());
    }

    @Override
    public Void visit(RestServiceAction.RetrieveSingleDocument retrieveSingleDocument) {
        final CompletableFuture<BsonObject> documentFuture = retrieveSingleDocument.perform();
        final BsonObject document;
        try {
            document = documentFuture.get();
            rc.response()
                    .putHeader("content-type", "application/json")
                    .end(document.encodeToString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public Void visit(RestServiceAction.ExecuteCommand executeCommand) {
        executeCommand.perform();
        rc.response().setStatusCode(200).end();
        return null;
    }

    @Override
    public Void visit(RestServiceAction.ListDocumentIds listDocumentIds) {
        return null;
    }

    @Override
    public Void visit(RestServiceAction.ListBinders listBinders) {
        return null;
    }

    @Override
    public Void visit(RestServiceAction.RunQuery runQuery) {
        return null;
    }

}
