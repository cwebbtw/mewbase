package io.mewbase.eventsource.impl;


import io.mewbase.eventsource.channels.ChannelAccessException;
import io.mewbase.eventsource.channels.ChannelAccessRegistry;




public class SecurePublishDelegate  {

    final ChannelAccessRegistry reg = ChannelAccessRegistry.instance();

    void checkPublish(String channelName) {
        if (!reg.canPublishTo(channelName)) throw new ChannelAccessException("Can't publish to "+channelName);
        return;
    }


}
