package io.mewbase.eventsource.channels;


import com.typesafe.config.ConfigFactory;
import io.mewbase.MewbaseTestBase;
import io.mewbase.TestUtils;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.channels.impl.AllAccessRegistry;
import io.mewbase.eventsource.channels.impl.NoAccessRegistry;
import org.junit.Test;


import static org.junit.Assert.*;


/**
 * Created by Nige on 17/5/2018.
 */

public class ChannelAccessTest extends MewbaseTestBase {

    @Test
    public void testDefaultIsAllAccess() throws Exception {
        ChannelAccessRegistry reg = ChannelAccessRegistry.instance();
        // cast will hurl if it is not an all access registry
        AllAccessRegistry allReg = (AllAccessRegistry)reg;
        String randomChannel = TestUtils.randomString(16);
        assertTrue( allReg.canPublishTo(randomChannel) );
        assertTrue( allReg.canSubscribeTo(randomChannel) );
     }


    @Test
    public void testNoOrPoorConfigGivesNoAccess() throws Exception {
        setUpBadConfig();
        ChannelAccessRegistry reg = ChannelAccessRegistry.instance();
        // cast will hurl if it is not a NoAccess registry
        NoAccessRegistry allReg = (NoAccessRegistry)reg;
        String randomChannel = TestUtils.randomString(16);
        assertFalse( allReg.canPublishTo(randomChannel) );
        assertFalse( allReg.canSubscribeTo(randomChannel) );
    }

    @Test
    public void testFileEventSinkDefaultAccess() throws Exception {
        EventSink sink = EventSink.instance();
        BsonObject event = new BsonObject().put("age", 27);
        // if this doesnt work it will throw a channel access exception
        Long eventNumber = sink.publishSync("OpenChannel", event );
        assertNotNull(eventNumber);

    }


    private void setUpBadConfig() {
        // over-ride the good settings to make a JMS sink
        final String accessFactory = "mewbase.event.channels.access.factory";
        System.setProperty(accessFactory, "not.a.valid.class");
        // force reload of config values
        ConfigFactory.invalidateCaches();
        ConfigFactory.load();
        // cache side effects be gone
    }

}