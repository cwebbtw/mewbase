package io.mewbase.util;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Cant do this with an interface due to diamond inheritance probs.
 * See https://stackoverflow.com/questions/7486012/static-classes-in-java#7486111
 */
public final class CanFactoryFrom {

        static Logger log = LoggerFactory.getLogger(CanFactoryFrom.class);

        /** Static use only */
        private CanFactoryFrom() {  }

        public static <T>  T instance(final String implFQCN, final Config cfg, Supplier<T> defaultImpl) {
            try {
                final T impl = (T)Class.forName(implFQCN).getDeclaredConstructor(Config.class).newInstance(cfg);
                if (impl == null) throw new Exception("Attempt to factory "+implFQCN+" resulted in null");
                return impl;
            } catch (Exception exp ) {
                log.error("Factory failed - check config of "+implFQCN + " making default implementation.", exp);
                return defaultImpl.get();
            }
        }

    }
