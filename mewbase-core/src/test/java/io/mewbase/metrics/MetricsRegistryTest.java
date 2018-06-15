package io.mewbase.metrics;



import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

/**
 * Created by Nige on 30/09/16.
 */

public class MetricsRegistryTest {

    @Test
    public void testProjectionFactory()  {
        String s = MetricsRegistry.discoverAllMetrics();
        System.out.println(s);
    }


}



