package io.mewbase.rest;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.Query;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.metrics.MetricsRegistry;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public abstract class RestServiceAction<Res> {

    private RestServiceAction() {
    }

    public interface Visitor<VisitorRes> {
        VisitorRes visit(RetrieveSingleDocument retrieveSingleDocument);
        VisitorRes visit(ExecuteCommand executeCommand);
        VisitorRes visit(ListDocumentIds listDocumentIds);
        VisitorRes visit(ListBinders listBinders);
        VisitorRes visit(RunQuery runQuery);
        VisitorRes visit(GetMetrics getMetrics);
    }

    public static RetrieveSingleDocument retrieveSingleDocument(BinderStore binderStore, String binderName, String documentId) {
        return new RetrieveSingleDocument(binderStore, binderName, documentId);
    }

    public static ExecuteCommand executeCommand(CommandManager commandManager, String commandName, BsonObject context) {
        return new ExecuteCommand(commandManager, commandName, context);
    }

    public static ListDocumentIds listDocumentIds(BinderStore binderStore, String binderName) {
        return new ListDocumentIds(binderStore, binderName);
    }

    public static ListBinders listBinders(BinderStore binderStore) {
        return new ListBinders(binderStore);
    }

    public static RunQuery runQuery(QueryManager queryManager, String queryName, BsonObject context) {
        return new RunQuery(queryManager, queryName, context);
    }

    public static GetMetrics getMetrics() {
        return new GetMetrics();
    }

    public abstract <VisitorRes> VisitorRes visit(Visitor<VisitorRes> visitor);
    public abstract Res perform();

    public static final class ListBinders extends RestServiceAction<Stream<String>> {
        private final BinderStore binderStore;

        public ListBinders(BinderStore binderStore) {
            this.binderStore = binderStore;
        }

        public BinderStore getBinderStore() {
            return binderStore;
        }

        @Override
        public <VisitorRes> VisitorRes visit(Visitor<VisitorRes> visitor) { return visitor.visit(this); }

        @Override
        public Stream<String> perform() {
            return binderStore.binderNames();
        }
    }

    public static final class ListDocumentIds extends RestServiceAction<Stream<String>> {
        private final BinderStore binderStore;
        private final String binderName;

        public ListDocumentIds(BinderStore binderStore, String binderName) {
            this.binderStore = binderStore;
            this.binderName = binderName;
        }

        public BinderStore getBinderStore() {
            return binderStore;
        }

        public String getBinderName() {
            return binderName;
        }

        @Override
        public <VisitorRes> VisitorRes visit(Visitor<VisitorRes> visitor) { return visitor.visit(this); }

        @Override
        public Stream<String> perform() {
            final Binder binder = getBinderStore().open(getBinderName());
            final Stream<KeyVal<String, BsonObject>> documents = binder.getDocuments();
            return documents.map(KeyVal::getKey);
        }

    }

    public static final class RetrieveSingleDocument extends RestServiceAction<CompletableFuture<BsonObject>> {
        private final BinderStore binderStore;
        private final String binderName;
        private final String documentId;

        public RetrieveSingleDocument(BinderStore binderStore, String binderName, String documentId) {
            this.binderStore = binderStore;
            this.binderName = binderName;
            this.documentId = documentId;
        }

        public BinderStore getBinderStore() {
            return binderStore;
        }

        public String getBinderName() {
            return binderName;
        }

        public String getDocumentId() {
            return documentId;
        }

        @Override
        public <VisitorRes> VisitorRes visit(Visitor<VisitorRes> visitor) {
            return visitor.visit(this);
        }

        @Override
        public CompletableFuture<BsonObject> perform() {
            final Binder binder = getBinderStore().open(getBinderName());
            return binder.get(getDocumentId());
        }
    }

    public static final class ExecuteCommand extends RestServiceAction<CompletableFuture<Long>> {
        private final CommandManager commandManager;
        private final String commandName;
        private final BsonObject context;

        public ExecuteCommand(CommandManager commandManager, String commandName, BsonObject context) {
            this.commandManager = commandManager;
            this.commandName = commandName;
            this.context = context;
        }

        public CommandManager getCommandManager() {
            return commandManager;
        }

        public String getCommandName() {
            return commandName;
        }

        public BsonObject getContext() {
            return context;
        }

        @Override
        public <Res> Res visit(Visitor<Res> visitor) {
            return visitor.visit(this);
        }

        @Override
        public CompletableFuture<Long> perform() {
            return getCommandManager().execute(getCommandName(), getContext());
        }
    }

    public static final class RunQuery extends RestServiceAction<Optional<BsonObject>> {

        private final QueryManager queryManager;
        private final String queryName;
        private final BsonObject context;

        public RunQuery(QueryManager queryManager, String queryName, BsonObject context) {
            this.queryManager = queryManager;
            this.queryName = queryName;
            this.context = context;
        }

        @Override
        public <VisitorRes> VisitorRes visit(Visitor<VisitorRes> visitor) { return visitor.visit(this); }

        @Override
        public Optional<BsonObject> perform() {
            final Optional<Query> queryOpt = getQueryManager().getQuery(getQueryName());
            return queryOpt.map(query -> {
               final Stream<KeyVal<String, BsonObject>> result = query.execute(getContext());
               return BsonObject.from(result);
            });
        }

        public QueryManager getQueryManager() {
            return queryManager;
        }

        public String getQueryName() {
            return queryName;
        }

        public BsonObject getContext() {
            return context;
        }
    }

    public static final class GetMetrics extends RestServiceAction<BsonObject> {

        public GetMetrics() {
            MetricsRegistry.ensureRegistry();
        }

        @Override
        public <VisitorRes> VisitorRes visit(Visitor<VisitorRes> visitor) {
            return visitor.visit(this);
        }

        @Override
        public BsonObject perform() {
            return MetricsRegistry.allMetricsAsDocument();
        }

    }

}
