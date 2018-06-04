package io.mewbase.rest;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.binders.BinderStore;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.rest.impl.VertxRestServiceAdaptor;
import io.mewbase.util.CanFactoryFrom;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
        return CanFactoryFrom.instance(cfg.getString(factoryConfigPath), cfg, () -> new VertxRestServiceAdaptor(cfg) );
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

    RestServiceAdaptor exposeGetDocument(BinderStore binderStore, String uri, Function<IncomingRequest, DocumentLookup> requestToDocumentLookup);

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
     * Todo
     * @param qmgr
     * @param commandName
     * @return
     */
    RestServiceAdaptor exposeCommand(CommandManager qmgr, String commandName);
    RestServiceAdaptor exposeCommand(CommandManager qmgr, String commandName, String uriPathPrefix);

    /**
     * Todo
     * @param qmgr
     * @param queryName
     * @return
     */
    RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName);
    RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName, String uriPathPrefix);


    /**
     * Start this REST adapter asynchronously.
     * Multiple rest adapters may be started on the same host and
     * bound to different endpoints (ipaddr:port) combinations.
     * @return
     */
    CompletableFuture<Void> start();

    /**
     * Stop this REST adapter asynchronously.
     * @return
     */
    CompletableFuture<Void> stop();

    @Override
    void close();

}
