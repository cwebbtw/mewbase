package io.mewbase.rest;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.binders.BinderStore;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.util.CanFactoryFrom;

import java.util.concurrent.CompletableFuture;

/**
 * Adapted by Nige on 19/12/17.
 * From Tim's original Impl 16/12/16.
 */
public interface RestServiceAdaptor extends AutoCloseable {


    String factoryConfigPath = "mewbase.api.rest.factory";

    /**
     * Given the contents of the current config create a concrete instance of a RestServiceAdapter
     * @return
     */
    static RestServiceAdaptor instance() {
        return RestServiceAdaptor.instance(ConfigFactory.load());
    }

    /**
     * Given the local config make a concrete instance of a RestServiceAdapter
     * @param cfg
     * @return
     */
    static RestServiceAdaptor instance(Config cfg) {
        return CanFactoryFrom.instance(cfg.getString(factoryConfigPath), cfg);
    }

    /**
     * This will setup a set of routes (paths) to every document in every binder in the form
     *
     * Verb : GET
     * URI : http://Server:Port/binders/{binderName}/{binderId}
     *
     * In the case where both binderName and binderId is missing ...
     *
     * Hence -> http://http://Server:Port/binders
     *
     * The call will list all of the binder names defined in the current context.
     *
     * In the case where a valid binderName is given it will list all of the documentIds for that binder.
     *
     * In the case where both the binderId and the documentIds is given
     *
     * This method is functionally equivalent to calling exposeGetDocument(binderStore, "/binders" );
     *
     * @param binderStore
     * @return
     */
    RestServiceAdaptor exposeGetDocument(BinderStore binderStore);


    /**
     * This method is functionally the similar to @exposeGetDocument(BinderStore binderStore) however it can be
     * used to insert a URI path prefix that subsumes the 'binders' path hence ...
     *
     * if uriPathPrefix == "/recipes" then URI for accessing the binders would be contextualised to
     *
     * Verb : GET
     * URI : http://Server:Port/recipes/{binderName}/{binderId}
     *
     *
     * If the uriPathPrefix is the empty string "" the binder name will become the first path element.
     *
     * @param binderStore
     * @param uriPathPrefix
     * @return
     */
    RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String uriPathPrefix);

    /**
     * This method is exposes only the named Binder in the binder store and all of its contained
     * documents. It places a single limit on the scope of the binders that can be accessed.
     *
     * Path prefix behaviour is the same as for above.
     *
     * @param binderStore
     * @param uriPathPrefix
     * @return
     */
    RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String binderName, String uriPathPrefix);


    /**
     * Expose a call to a command defined in the given command manager
     * with the given command name.
     *
     * @param cmgr - The command manager
     * @param commandName - The command name
     * @return the adapter instance.
     */
    RestServiceAdaptor exposeCommand(CommandManager cmgr, String commandName);


    /**
     * Expose a call to a command defined in the given command manager
     * with the given command name.
     *
     * @param cmgr - The command manager
     * @param commandName - The command name
     * @param uriPathPrefix
     * @return the adapter instance.
     */
    RestServiceAdaptor exposeCommand(CommandManager cmgr, String commandName, String uriPathPrefix);

    /**
     * Expose a call to a query defined in the given query manager
     * with the given query name.
     * @param qmgr - The query manager
     * @param queryName - query name
     * @return the adapter instance
     */
    RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName);


    /**
     * Expose a call to a query defined in the given query manager
     * with the given query name.
     * @param qmgr - The query manager
     * @param queryName - query name
     * @param uriPathPrefix
     * @return the adapter instance
     */
    RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName, String uriPathPrefix);


    /**
     * Expose the ability to get all of the server and mewbase metrics that have been recorded
     * as a JSON response to a get request.
     *
     * Verb : GET
     * URI : http://Server:Port/metrics
     *
     * Please note that this reserves the above URI in the name space hence calling a binder by the same name
     * with the same path prefix will result in undefined behaviour. I.e. one of the two will be called depending
     * on the "route resolution" in the underlying REST implementation.
     *
     * @return the adapter instance
     */
    RestServiceAdaptor exposeMetrics();


    /**
     * As exposeMetrics (as above) with a path prefix
     * @param uriPathPrefix
     * @return the adapter instance
     */
    RestServiceAdaptor exposeMetrics(String uriPathPrefix);

    /**
     * Start this REST adapter asynchronously.
     * Multiple rest adapters may be started on the same host and
     * bound to different endpoints (ipaddr:port) combinations.
     * @return
     */
    CompletableFuture<Void> start();

    int getServerPort();

    /**
     * Stop this REST adapter asynchronously.
     * @return
     */
    CompletableFuture<Void> stop();

    @Override
    void close();

}
