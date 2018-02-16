package io.mewbase.eventsource.impl.jms;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.MewbaseTestBase;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.impl.file.FileEventSink;
import io.mewbase.eventsource.impl.file.FileEventUtils;
import org.junit.Test;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;



/**
 * Created by Nige on 16/2/2018.
 */
public class JMSEventSinkTest extends MewbaseTestBase {


    @Test
    public void testCreatesJMSEventSink() throws Exception {

        // create the test config to set up the file paths for this test
        final Config cfg = createConfig();

        // create a bridge from a JMS to a file based event Sink
        // JmsChannelTunnel tunnel = new JmsChannelTunnel();

        // over-ride the sink settings to make a JMS sink
        final String sinkProp = "mewbase.event.sink.";
        System.setProperty(sinkProp + "factory", "io.mewbase.eventsource.impl.jms.JmsEventSink");
        final String jmsSinkProp = sinkProp + "jms.";
        System.setProperty(jmsSinkProp + "connectionFactoryFQCN", "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory" );
        System.setProperty(jmsSinkProp + "serverUrl", "tcp://localhost:61616" );
        System.setProperty(jmsSinkProp + "username", "admin" );
        System.setProperty(jmsSinkProp + "password", "admin" );
        // force reload of config values
        ConfigFactory.invalidateCaches();
        ConfigFactory.load();
        // cache side effects be gone


        EventSink sink = EventSink.instance();
        assertNotNull(sink);
        // cast for type will hurl if not that type
        JmsEventSink jmsSink =(JmsEventSink)sink;
    }

   // @Test
    public void testJMSPublishesEvent() throws Exception {

        // TODO Copy first test down
        // enable tunnel
        // check message goes though whole pipeline
    }



}
