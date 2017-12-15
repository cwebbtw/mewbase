package io.mewbase;



import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;


/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class ConfigTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(ConfigTest.class);

    @Test
    public void testConfig () throws Exception {

        Config cfg = ConfigFactory.load();
        String sinkFactory = cfg.getString("mewbase.event.source.factory");
        assertEquals("io.mewbase.eventsource.impl.nats.NatsEventSource",sinkFactory);

    }


}
