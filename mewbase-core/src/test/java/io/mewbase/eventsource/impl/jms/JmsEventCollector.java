package io.mewbase.eventsource.impl.jms;


import io.mewbase.bson.BsonCodec;
import io.mewbase.bson.BsonObject;

import javax.jms.*;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;


public class JmsEventCollector {

        private final List<BsonObject> list = new ArrayList<BsonObject>();

        @SuppressWarnings("unchecked")
        public JmsEventCollector(final String channelName) throws Exception {

            /* Set up a consumer for the JMS queue channel */
            final Class factoryClass = Class.forName("org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory");
            final Constructor<ConnectionFactory> ctor = factoryClass.getDeclaredConstructor(String.class);
            final ConnectionFactory jmsFactory =  ctor.newInstance("tcp://localhost:61616");
            final Connection connection = jmsFactory.createConnection("admin", "admin");
            final Session jmsSession = connection.createSession(javax.jms.Session.AUTO_ACKNOWLEDGE);
            final Destination destination = jmsSession.createQueue(channelName);
            final MessageConsumer consumer = jmsSession.createConsumer(destination);

            consumer.setMessageListener(message -> {
                try {
                    BytesMessage byteMsg = (BytesMessage) message;
                    byte[] body = new byte[(int) byteMsg.getBodyLength()];
                    ((BytesMessage) message).readBytes(body);

                    // Store the event for test purposes
                    list.add(BsonCodec.bsonBytesToBsonObject(body));
                } catch(Exception exp ) {
                    System.out.println("Boom " + exp.toString());
                }
            });
            connection.start();
        }

    public long eventCount() { return list.size(); }

    public Stream<BsonObject> events()  { return list.stream(); }
}
