package io.mewbase.rest;

import io.mewbase.binders.BinderStore;

public abstract class RestServiceAction {

    private RestServiceAction() {
    }

    public static interface Visitor<Res> {
        Res visit(RetrieveSingleDocument retrieveSingleDocument);
    }

    public abstract <Res> Res visit(Visitor<Res> visitor);

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

}
