package io.mewbase.rest;

import java.util.function.Function;

public interface RestAdapter<Request, Response, Service> {

    Service adapt(Function<Request, Response> requestMapper);

}
