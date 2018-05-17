package io.mewbase.eventsource.channels;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.channels.impl.NoAccessRegistry;
import io.mewbase.util.CanFactoryFrom;




public interface ChannelAccessRegistry {


    String factoryConfigPath = "mewbase.event.channels.access.factory";
    /**
     * Create an instance using the current config.
     * @return an Instance of a ChannelAccessRegistery
     */
    static ChannelAccessRegistry instance() {
        return ChannelAccessRegistry.instance(ConfigFactory.load());
    }

    /**
     * Create an instance using the current config.
     * If the config fails it will create a stub Resistry
     * @return an Instance of an EventSink
     */
    static ChannelAccessRegistry instance(Config cfg) {
        return CanFactoryFrom.instance(cfg.getString(factoryConfigPath), cfg, () -> new NoAccessRegistry(cfg) );
    }

    /**
     * Mostly for consumption by EventSink but can be used to check access before calling
     * publish methods
     * @param channelName
     * @return
     */
  Boolean canPublishTo(String channelName);


    /**
     * Mostly for consumption by EventSource but can be used to check access before calling
     * subscribe methods
     * @param channelName
     * @return
     */
  Boolean canSubscribeTo(String channelName);


}
