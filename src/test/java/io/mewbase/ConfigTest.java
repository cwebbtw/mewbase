package io.mewbase;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;


/**
 * Created by Nige on 15/12/17.
 */
@RunWith(VertxUnitRunner.class)
public class ConfigTest extends MewbaseTestBase {


    @Test
    public void testConfig () throws Exception {

        Config cfg = ConfigFactory.load();
        String sinkFactory = cfg.getString("mewbase.event.source.factory");
        assertEquals("io.mewbase.eventsource.impl.nats.NatsEventSource",sinkFactory);
    }


}
