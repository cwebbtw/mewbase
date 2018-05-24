package io.mewbase.eventsource.channels.impl;


import com.typesafe.config.Config;
import io.mewbase.eventsource.channels.ChannelAccessRegistry;


public class AllAccessRegistry implements ChannelAccessRegistry {

    public AllAccessRegistry(Config cfg) {

    }


    @Override
    public Boolean canPublishTo(String channelName) {
        return true;
    }

    @Override
    public Boolean canSubscribeTo(String channelName) {
        return true;
    }

}
