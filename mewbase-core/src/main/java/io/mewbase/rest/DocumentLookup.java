package io.mewbase.rest;

import java.util.Objects;

public class DocumentLookup {
    private final String binderName;
    private final String documentId;

    public DocumentLookup(String binderName, String documentId) {
        this.binderName = binderName;
        this.documentId = documentId;
    }

    public String getBinderName() {
        return binderName;
    }

    public String getDocumentId() {
        return documentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocumentLookup that = (DocumentLookup) o;
        return Objects.equals(binderName, that.binderName) &&
                Objects.equals(documentId, that.documentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(binderName, documentId);
    }

    @Override
    public String toString() {
        return "DocumentLookup{" +
                "binderName='" + binderName + '\'' +
                ", documentId='" + documentId + '\'' +
                '}';
    }
}
