package io.mewbase.eventsource.impl;


import io.mewbase.eventsource.channels.ChannelAccessException;
import io.mewbase.eventsource.channels.ChannelAccessRegistry;


public class SecureSubscribeDelegate {

    final ChannelAccessRegistry reg = ChannelAccessRegistry.instance();

    void checkSubscribe(String channelName) {
        if (!reg.canSubscribeTo(channelName)) throw new ChannelAccessException("Can't publish to "+channelName);
        return;
    }

}
