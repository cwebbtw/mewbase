package io.mewbase.rest.vertx;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonCodec;
import io.mewbase.bson.BsonObject;
import io.mewbase.rest.RestServiceAction;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class VertxRestServiceActionVisitor implements RestServiceAction.Visitor<Void> {

    private final RoutingContext rc;

    public VertxRestServiceActionVisitor(RoutingContext rc) {
        this.rc = rc;
    }

    public BsonObject bodyAsBson() {
        return rc.getBody().length() == 0 ? new BsonObject() : BsonCodec.jsonStringToBsonObject(rc.getBodyAsString());
    }

    private void sendResponse(Stream<String> response) {
        final BsonArray result = BsonArray.from(response);
        rc.response()
                .putHeader("content-type", "application/json")
                .end(BsonCodec.bsonArrayToJsonArray(result).toString());
    }

    private void sendResponse(BsonObject bsonObject) {
        rc.response()
                .putHeader("content-type", "application/json")
                .end(BsonCodec.bsonObjectToJsonObject(bsonObject).toString());
    }

    @Override
    public Void visit(RestServiceAction.RetrieveSingleDocument retrieveSingleDocument) {
        final CompletableFuture<BsonObject> documentFuture = retrieveSingleDocument.perform();
        final BsonObject document;
        try {
            document = documentFuture.get();
            rc.response()
                    .putHeader("content-type", "application/json")
                    .end(BsonCodec.bsonObjectToJsonObject(document).toString());
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
        final Stream<String> documentIds = listDocumentIds.perform();
        sendResponse(documentIds);
        return null;
    }

    @Override
    public Void visit(RestServiceAction.ListBinders listBinders) {
        final Stream<String> binders = listBinders.perform();
        sendResponse(binders);
        return null;
    }

    @Override
    public Void visit(RestServiceAction.RunQuery runQuery) {
        final Optional<BsonObject> result = runQuery.perform();

        if (result.isPresent())
            sendResponse(result.get());
        else
            rc.response()
                    .setStatusCode(404)
                    .setStatusMessage("Not Found")
                    .end("Query not found");
        return null;
    }

    @Override
    public Void visit(RestServiceAction.GetMetrics getMetrics) {
        final BsonObject metrics = getMetrics.perform();

        rc.response()
                .putHeader("Content-Type", "application/json")
                .end(BsonCodec.bsonObjectToJsonObject(metrics).toString());

        return null;
    }

}
