package io.mewbase.server;

import io.mewbase.cqrs.QueryBuilder;
import io.mewbase.cqrs.CommandBuilder;
import io.vertx.core.http.HttpMethod;

/**
 * Created by tim on 15/12/16.
 */
public interface Mewbase {

    // Binder related operations

    // CompletableFuture<Binder> createBinder(String binderName);

    // CompletableFuture<Binder> getBinder(String name);

    // Stream<String> listBinders();


    // Projection related operations

    // ProjectionBuilder buildProjection(String projectionName);

    // List<String> listProjections();

    // Projection getProjection(String projectionName);


    // Command handler related operations

 //   CommandBuilder buildCommandHandler(String commandName);


    // Query related operations

//    QueryBuilder buildQuery(String queryName);


    // REST adaptor related operations

//    Mewbase exposeCommand(String commandName, String uri, HttpMethod httpMethod);
//
//    Mewbase exposeQuery(String queryName, String uri);
//
//    Mewbase exposeFindByID(String binderName, String uri);

}
