package io.mewbase.eventsource.impl.jms;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;


import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;


/**
 * Created by Nige on 16/2/2018.
 */
public class JMSEventSinkTest extends MewbaseTestBase {

    // Requires Apache Artemis to be running see mewbase wiki
    @Test
    public void testCreatesJMSEventSink()  {

        // over-ride the sink settings to make a JMS sink
        setUpJMSConfiguration();

        final EventSink sink = EventSink.instance();

        assertNotNull(sink);
        // cast for type will hurl if not that type
        final JmsEventSink jmsSink =(JmsEventSink)sink;

        jmsSink.close();
    }

    // Requires Apache Artemis to be runing see mewbase wiki
    @Test
    public void testJMSPublishesEvent() throws Exception {

        // create the test config to set up the file paths for this test
        final String eventDataKey = "data";

        final String testChannelName = "JMSTestChannel" + UUID.randomUUID();
        // create a bridge from a JMS to a file based event Sink
        JmsEventCollector collector = new JmsEventCollector(testChannelName);
        setUpJMSConfiguration();

        final JmsEventSink jmsSink = (JmsEventSink) EventSink.instance();

        final String inputUUID = UUID.randomUUID().toString();
        final BsonObject bsonEvent = new BsonObject().put(eventDataKey, inputUUID);

        final long eventNumber = jmsSink.publishSync(testChannelName, bsonEvent);
        assertEquals(-1, eventNumber);

        Thread.sleep(200);

        assertEquals(1, collector.eventCount());
        assertEquals( inputUUID, collector.events().findFirst().get().getString(eventDataKey));

        jmsSink.close();
    }


    @Test
    public void testJMSAsyncInOrder() throws Exception {

        final String eventDataKey = "data";
        final String testChannelName = "JMSTestChannel" + UUID.randomUUID();

        // create a bridge from a JMS to a file based event Sink
        JmsEventCollector collector = new JmsEventCollector(testChannelName);
        setUpJMSConfiguration();

        final JmsEventSink jmsSink = (JmsEventSink) EventSink.instance();

        List<BsonObject> events = new LinkedList<>();

        IntStream.range(0,10).sequential().
                mapToObj(in-> UUID.randomUUID().toString()).
                        forEach(  (String uuidStr) -> {
                            final BsonObject bsonEvent = new BsonObject().put(eventDataKey, uuidStr);
                            jmsSink.publishAsync(testChannelName, bsonEvent);
                            events.add(bsonEvent);
                        });

        Thread.sleep(200);

        Iterator<BsonObject> collectedEvents = collector.events().iterator();
        events.forEach( evt -> assertEquals( evt, collectedEvents.next()));

        jmsSink.close();
    }

    private void setUpJMSConfiguration() {
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
    }

}
