package io.mewbase.eventsource.channels.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.channels.ChannelAccess;
import io.mewbase.eventsource.channels.ChannelAccessRegistry;

import java.util.HashMap;
import java.util.Map;

public class LocalAccessRegistry implements ChannelAccessRegistry {


    final Map<String,ChannelAccess> accesses  = new HashMap<String,ChannelAccess>();


    public LocalAccessRegistry() {
        this(ConfigFactory.load());
    }

    public LocalAccessRegistry(Config cfg) {

    }


    @Override
    public Boolean canPublishTo(String channelName) {
        final ChannelAccess access = getAccessFor(channelName);
        return access == ChannelAccess.PUBLISH || access == ChannelAccess.PUBSUB;
    }


    @Override
    public Boolean canSubscribeTo(String channelName) {
        final ChannelAccess access = getAccessFor(channelName);
        return access == ChannelAccess.SUBSCRIBE || access == ChannelAccess.PUBSUB;
    }


    public ChannelAccess putAccess(String channelName, ChannelAccess access) {
        return accesses.put(channelName, access);
    }

    public ChannelAccess getAccessFor(String channelName) {
        return accesses.getOrDefault(channelName, ChannelAccess.NONE);
    }


}
