package io.mewbase.rest.vertx;

import io.mewbase.bson.BsonObject;
import io.mewbase.rest.RestAdapter;
import io.mewbase.rest.RestServiceAction;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

public class VertxRestServiceActionVisitor {

    public static RestServiceAction.Visitor<Void> actionVisitor(HttpServerResponse response) {
        return new RestServiceAction.Visitor<Void>() {
            @Override
            public Void visit(RestServiceAction.RetrieveSingleDocument retrieveSingleDocument) {
                final CompletableFuture<BsonObject> documentFuture = retrieveSingleDocument.perform();
                final BsonObject document;
                try {
                    document = documentFuture.get();
                    response
                            .putHeader("content-type", "application/json")
                            .end(document.encodeToString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }

            @Override
            public Void visit(RestServiceAction.ExecuteCommand executeCommand) {
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
        };
    }
}
