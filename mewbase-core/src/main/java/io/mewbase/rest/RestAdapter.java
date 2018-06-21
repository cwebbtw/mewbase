package io.mewbase.rest;

import java.util.function.Function;

public interface RestAdapter<Request, Service> {

    Service adapt(Function<Request, RestServiceAction> requestMapper);

}
