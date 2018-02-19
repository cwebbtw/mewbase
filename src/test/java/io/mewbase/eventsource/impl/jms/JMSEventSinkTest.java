package io.mewbase.eventsource.impl.jms;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;


/**
 * Created by Nige on 16/2/2018.
 */
public class JMSEventSinkTest extends MewbaseTestBase {


    @Test
    public void testCreatesJMSEventSink()  {

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

        final EventSink sink = EventSink.instance();
        assertNotNull(sink);
        // cast for type will hurl if not that type
        final JmsEventSink jmsSink =(JmsEventSink)sink;
        jmsSink.close();
    }


    @Test
    public void testJMSPublishesEvent() throws Exception {

        // create the test config to set up the file paths for this test
        final Config cfg = createConfig();

        // create a bridge from a JMS to a file based event Sink
        JmsChannelTunnel tunnel = new JmsChannelTunnel();

        // over-ride the sink settings to make a JMS sink
        final String sinkProp = "mewbase.event.sink.";
        System.setProperty(sinkProp + "factory", "io.mewbase.eventsource.impl.jms.JmsEventSink");
        final String jmsSinkProp = sinkProp + "jms.";
        System.setProperty(jmsSinkProp + "connectionFactoryFQCN", "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory");
        System.setProperty(jmsSinkProp + "serverUrl", "tcp://localhost:61616");
        System.setProperty(jmsSinkProp + "username", "admin");
        System.setProperty(jmsSinkProp + "password", "admin");
        // force reload of config values
        ConfigFactory.invalidateCaches();
        ConfigFactory.load();
        // cache side effects be gone

        final JmsEventSink jmsSink = (JmsEventSink) EventSink.instance();
        final EventSource source = EventSource.instance(cfg);

        // make the channel unique so that the event number is always zero below.
        final String testChannelName = JmsChannelTunnel.testChannelName;
        final String inputUUID = randomString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        // check the event arrived
        final CountDownLatch latch = new CountDownLatch(1);

        Subscription subs = source.subscribe(testChannelName, event -> {
                    BsonObject bson = event.getBson();
                    assert (inputUUID.equals(bson.getString("data")));
                    latch.countDown();
                }
        );

        long eventNumber = jmsSink.publishSync(testChannelName, bsonEvent);
        // JMS doesnt understand event numbers at the client (sink) side
        assertEquals(-1, eventNumber);
        Thread.sleep(200);
        latch.await();
        // Check a message went through the tunnel
        assertEquals(1, tunnel.messageCount());

        source.close();
        jmsSink.close();
    }
}
