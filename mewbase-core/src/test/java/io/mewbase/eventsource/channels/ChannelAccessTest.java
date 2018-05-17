package io.mewbase.eventsource.channels;


import com.typesafe.config.ConfigFactory;
import io.mewbase.MewbaseTestBase;
import io.mewbase.TestUtils;
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
        // cast will hurl if it is not an all access registry
        NoAccessRegistry allReg = (NoAccessRegistry)reg;
        String randomChannel = TestUtils.randomString(16);
        assertFalse( allReg.canPublishTo(randomChannel) );
        assertFalse( allReg.canSubscribeTo(randomChannel) );
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