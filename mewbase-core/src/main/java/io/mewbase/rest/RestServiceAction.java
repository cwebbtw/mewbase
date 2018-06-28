package io.mewbase.rest;

import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;

public abstract class RestServiceAction {

    private RestServiceAction() {
    }

    public interface Visitor<Res> {
        Res visit(RetrieveSingleDocument retrieveSingleDocument);
        Res visit(ExecuteCommand executeCommand);
        Res visit(ListDocumentIds listDocumentIds);
        Res visit(ListBinders listBinders);
        Res visit(RunQuery runQuery);
    }

    public abstract <Res> Res visit(Visitor<Res> visitor);

    public static final class ListBinders extends RestServiceAction {
        private final BinderStore binderStore;

        public ListBinders(BinderStore binderStore) {
            this.binderStore = binderStore;
        }

        public BinderStore getBinderStore() {
            return binderStore;
        }

        @Override
        public <Res> Res visit(Visitor<Res> visitor) { return visitor.visit(this); }
    }

    public static final class ListDocumentIds extends RestServiceAction {
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
        public <Res> Res visit(Visitor<Res> visitor) { return visitor.visit(this); }

    }

    public static final class RetrieveSingleDocument extends RestServiceAction {
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
        public <Res> Res visit(Visitor<Res> visitor) {
            return visitor.visit(this);
        }
    }

    public static final class ExecuteCommand extends RestServiceAction {
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
    }

    public static final class RunQuery extends RestServiceAction {

        private final QueryManager queryManager;
        private final String queryName;
        private final BsonObject context;

        public RunQuery(QueryManager queryManager, String queryName, BsonObject context) {
            this.queryManager = queryManager;
            this.queryName = queryName;
            this.context = context;
        }

        @Override
        public <Res> Res visit(Visitor<Res> visitor) { return visitor.visit(this); }

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

}
