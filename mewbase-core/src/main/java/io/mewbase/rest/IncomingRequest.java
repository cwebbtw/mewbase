package io.mewbase.rest;

import java.util.Map;
import java.util.Objects;

public class IncomingRequest {
    private final Map<String, String> pathParameters;

    public IncomingRequest(Map<String, String> pathParameters) {
        this.pathParameters = pathParameters;
    }

    public Map<String, String> getPathParameters() {
        return pathParameters;
    }

    @Override
    public String toString() {
        return "IncomingRequest{" +
                "pathParameters=" + pathParameters +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IncomingRequest that = (IncomingRequest) o;
        return Objects.equals(pathParameters, that.pathParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathParameters);
    }
}
