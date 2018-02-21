package io.mewbase.eventsource.impl.jms;


import com.typesafe.config.ConfigFactory;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;


/**
 * Created by Nige on 16/2/2018.
 */
public class JMSEventSinkTest extends MewbaseTestBase {

    @Test  // Requires Apache Artemis to be runing see mewbase wiki
    public void testCreatesJMSEventSink()  {

        // over-ride the sink settings to make a JMS sink
        setUpJMSConfiguration();

        final EventSink sink = EventSink.instance();

        assertNotNull(sink);
        // cast for type will hurl if not that type
        final JmsEventSink jmsSink =(JmsEventSink)sink;

        jmsSink.close();
    }


    @Test  // Requires Apache Artemis to be runing see mewbase wiki
    public void testJMSPublishesEvent() throws Exception {

        // create the test config to set up the file paths for this test
        final String eventDataKey = "data";

        final String testChannelName = "JMSTestChannel" + UUID.randomUUID();
        JmsEventCollector collector = new JmsEventCollector(testChannelName);
        setUpJMSConfiguration();

        final JmsEventSink jmsSink = (JmsEventSink) EventSink.instance();

        final String inputUUID = UUID.randomUUID().toString();
        final BsonObject bsonEvent = new BsonObject().put(eventDataKey, inputUUID);

        final long eventNumber = jmsSink.publishSync(testChannelName, bsonEvent);
        assertEquals(-1, eventNumber);

        Thread.sleep(100);

        assertEquals(1, collector.eventCount());
        assertEquals( inputUUID, collector.events().findFirst().get().getString(eventDataKey));

        jmsSink.close();
    }


    @Test  // Requires Apache Artemis to be runing see mewbase wiki
    public void testJMSAsyncInOrder() throws Exception {

        final String eventDataKey = "data";

        final String testChannelName = "JMSTestChannel" + UUID.randomUUID();
        JmsEventCollector collector = new JmsEventCollector(testChannelName);
        setUpJMSConfiguration();

        final JmsEventSink jmsSink = (JmsEventSink) EventSink.instance();

        final int totalEvents = 64;

        final List<BsonObject> eventsToSend = IntStream.range(0,totalEvents).
                sequential().
                mapToObj( i -> UUID.randomUUID().toString() ).
                map( uuidStr -> new BsonObject().put(eventDataKey, uuidStr)).
                collect(Collectors.toList());

        final List<CompletableFuture<Long>> futs = eventsToSend.stream().sequential().
                map( evt -> jmsSink.publishAsync(testChannelName, evt) ).
                collect(Collectors.toList());

        // collect all the completed futures
        futs.forEach( f -> f.join() );

        // give the events time to be collected.
        Thread.sleep(500);

        Iterator<BsonObject> collectedEvents = collector.events().iterator();
        eventsToSend.forEach( evt -> assertEquals( evt, collectedEvents.next()));

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
