package io.mewbase.eventsource.impl.jms;


import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import javax.jms.*;
import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicLong;


public class JmsChannelTunnel {

        public static final String testChannelName = "tunnelTestChannel";

        AtomicLong messageCount = new AtomicLong(0);


        public JmsChannelTunnel() throws Exception {

            /* Set up a consumer for the JMS queue channel */
            final Class factoryClass = Class.forName("org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory");
            final Constructor<ConnectionFactory> ctor = factoryClass.getDeclaredConstructor(String.class);
            final ConnectionFactory jmsFactory =  ctor.newInstance("tcp://localhost:61616");
            final Connection connection = jmsFactory.createConnection("admin", "admin");
            final Session jmsSession = connection.createSession(javax.jms.Session.AUTO_ACKNOWLEDGE);
            final Destination destination = jmsSession.createQueue(testChannelName);
            final MessageConsumer consumer = jmsSession.createConsumer(destination);

            /* Set up the sink for the file system which is the default in the test config */
            EventSink sink = EventSink.instance();

            consumer.setMessageListener(message -> {
                try {
                    BytesMessage byteMsg = (BytesMessage) message;
                    byte[] body = new byte[(int) byteMsg.getBodyLength()];
                    ((BytesMessage) message).readBytes(body);

                    // Forward event to file based sink
                    BsonObject event = new BsonObject(body);
                    sink.publishAsync(testChannelName,event);
                    messageCount.getAndIncrement();

                } catch(Exception exp ) {
                    System.out.println("Boom " + exp.toString());
                }
            });
            connection.start();
        }

    public long messageCount() {
        return messageCount.get();
    }


}
