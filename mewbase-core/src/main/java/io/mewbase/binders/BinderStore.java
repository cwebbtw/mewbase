package io.mewbase.binders;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.binders.impl.BinderStoreShim;
import io.mewbase.binders.impl.filestore.FileBinderStore;
import io.mewbase.util.CanFactoryFrom;

import java.util.Optional;
import java.util.stream.Stream;


/**
 * Created by Nige on 14/09/17.
 */
public interface BinderStore extends AutoCloseable {

    String factoryConfigPath = "mewbase.binders.factory";

    /**
     * Create an instance using the current config.
     * @return an Instance of a BinderStore
     */
    static BinderStore instance() {
        return BinderStore.instance(ConfigFactory.load());
    }

    /**
     * Create an instance using the current config.
     * If the config fails it will create a FileBinderStore
     * @return an Instance of a BinderStore
     */
     static BinderStore instance(Config cfg) {
         BinderStore impl = CanFactoryFrom.instance(cfg.getString(factoryConfigPath), cfg, () -> new FileBinderStore(cfg));
         return new BinderStoreShim(impl);
     }


    /**
     * Open a new binder of the given name.
     *
     * If the binder doesnt already exist the binder wil be created.
     *
     * @param name of the Binder to open or create and open
     * @return succesfull  if Binder is created otherwise complet
     */
    Binder open(String name);

    /**
     * Get a Binder with the given name
     *
     * @param  name of the document within the binder
     * @return a CompletableFuture of the binder or a failed future if the binder doesnt exist.
     */
    Optional<Binder> get(String name);

    /**
     * Return a stream of the Binders so that maps / filters can be applied.
     *
     * @return a stream of all of the current binders
     */
    Stream<Binder> binders();

    /**
     * Return a stream of all of the names of the binders
     *
     * @return a stream of all of the current binder names.
     */
    Stream<String> binderNames();

    /**
     * Delete a binder from the store
     *
     * @param  name of  binder
     * @return a CompleteableFuture with a Boolean set to true if successful
     */
    @Deprecated
    Boolean delete(String name);

}
