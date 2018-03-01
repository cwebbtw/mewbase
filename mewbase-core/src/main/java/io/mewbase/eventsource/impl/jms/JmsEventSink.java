package io.mewbase.eventsource.impl.jms;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.jms.*;


public class JmsEventSink implements EventSink {


    final static String FACTORY_CONFIG_PATH = "mewbase.event.sink.jms.connectionFactoryFQCN";
    final static String SERVER_CONFIG_PATH = "mewbase.event.sink.jms.serverUrl";
    final static String USERNAME_CONFIG_PATH = "mewbase.event.sink.jms.username";
    final static String PASSWORD_CONFIG_PATH = "mewbase.event.sink.jms.password";

    final static Long SADLY_NO_CONCEPT_OF_A_MESSAGE_NUMBER = -1L;


    private final static Logger logger = LoggerFactory.getLogger(JmsEventSink.class);

    private final Executor exec = Executors.newSingleThreadExecutor();

    private Session jmsSession;

    public JmsEventSink() {
        this( ConfigFactory.load() );
    }


    public JmsEventSink(Config cfg) {

        try {
            final ConnectionFactory factory = factoryConnection(cfg);
            final String username = cfg.getString(USERNAME_CONFIG_PATH);
            final String password = cfg.getString(PASSWORD_CONFIG_PATH);
            final Connection connection = factory.createConnection(username, password);
            jmsSession = connection.createSession(javax.jms.Session.AUTO_ACKNOWLEDGE);
            logger.info("Created JmsEventSink for " + cfg.getString(FACTORY_CONFIG_PATH));
        } catch (Exception exp) {
            logger.error("Could not connect to JMS server for " + cfg.getString(FACTORY_CONFIG_PATH), exp);
        }
    }



    private final ConnectionFactory factoryConnection(Config cfg) throws Exception {
        try {
            final String factoryFQCN = cfg.getString(FACTORY_CONFIG_PATH);
            final Class factoryClass = Class.forName(factoryFQCN);
            final Constructor<ConnectionFactory> ctor = factoryClass.getDeclaredConstructor(String.class);

            final String serverUrl = cfg.getString(SERVER_CONFIG_PATH);
            final ConnectionFactory jmsFactory =  ctor.newInstance(serverUrl);

            if (jmsFactory == null) throw new Exception("Attempt to construct "+factoryFQCN+" resulted in null");
            return jmsFactory;
        } catch (Exception exp ) {
            logger.error("JmsConnectionFactory failed. Check config of "+FACTORY_CONFIG_PATH+" and "+SERVER_CONFIG_PATH, exp);
            throw exp;
        }
    }


    @Override
    public Long publishSync(String channelName, BsonObject event) {
            try {
               sendMessage(channelName, event);
            } catch (JMSException exp) {
                logger.error("Error sending event" + event, exp);
            }
            return SADLY_NO_CONCEPT_OF_A_MESSAGE_NUMBER;
    }


    @Override
    public CompletableFuture<Long> publishAsync(String channelName, BsonObject event) {
        // would like to use CompletableFuture.supplyAsync() but lambdas cant throw exceptions see
        // https://stackoverflow.com/questions/27644361/how-can-i-throw-checked-exceptions-from-inside-java-8-streams
        // so we have to exec on a thread
        CompletableFuture<Long> fut = new CompletableFuture<>();
        exec.execute( () -> {
            try { fut.complete( sendMessage(channelName,event)); }
            catch(Exception exp) { fut.completeExceptionally(exp); }
        });
        return fut;
    }


    /**
     * Encapsulate message send and capture exceptions
     */
    private final Long sendMessage(String channelName, BsonObject event) throws JMSException {
        final Destination destination = jmsSession.createQueue(channelName);
        final MessageProducer msgProducer = jmsSession.createProducer(destination);
        final BytesMessage msg = jmsSession.createBytesMessage();
        msg.writeBytes(event.encode().getBytes());
        msgProducer.send(msg);
        return SADLY_NO_CONCEPT_OF_A_MESSAGE_NUMBER;
    }

    @Override
    public void close() {
        try {
            jmsSession.close();
        } catch (Exception exp) {
            logger.error("Failed to close JMS session");
        }
    }

}
