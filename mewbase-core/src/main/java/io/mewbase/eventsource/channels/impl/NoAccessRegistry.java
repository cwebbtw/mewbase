package io.mewbase.eventsource.channels.impl;


import com.typesafe.config.Config;
import io.mewbase.eventsource.channels.ChannelAccessRegistry;


public class NoAccessRegistry implements ChannelAccessRegistry {

    public NoAccessRegistry(Config cfg) {

    }


    @Override
    public Boolean canPublishTo(String channelName) {
        return false;
    }

    @Override
    public Boolean canSubscribeTo(String channelName) {
        return false;
    }

}
