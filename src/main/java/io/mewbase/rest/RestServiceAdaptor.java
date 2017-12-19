package io.mewbase.rest;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


import io.mewbase.binders.BinderStore;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.rest.impl.VertxRestServiceAdaptor;
import io.mewbase.util.CanFactoryFrom;


import java.util.concurrent.CompletableFuture;

/**
 * Created by Nige on 19/12/17.
 * From Tim's original Impl 16/12/16.
 */
public interface RestServiceAdaptor {


    static RestServiceAdaptor instance() {
        return RestServiceAdaptor.instance(ConfigFactory.load());
    }

    static RestServiceAdaptor instance(Config cfg) {
        final String factoryConfigPath = "mewbase.api.rest.factory";
        return CanFactoryFrom.instance(cfg.getString(factoryConfigPath), () -> new VertxRestServiceAdaptor(cfg) );
    }


    RestServiceAdaptor exposeCommand(CommandManager qmgr, String commandName, String uri);


    RestServiceAdaptor exposeQuery(QueryManager qmgr, String queryName, String uri);


    RestServiceAdaptor exposeFindByID(BinderStore binderStore, String uri);


    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

}
