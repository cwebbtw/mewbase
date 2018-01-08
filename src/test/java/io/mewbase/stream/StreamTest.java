package io.mewbase.stream;

import io.mewbase.MewbaseTestBase;
import io.mewbase.binders.BinderStore;

import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;


/**
 * <p>
 * Created by Nige on 8/1/18.
 */
@RunWith(VertxUnitRunner.class)
public class StreamTest extends MewbaseTestBase {

    @Test
    public void testSimpleDeDupe() throws Exception {

        DeDuper dd = new DeDuper(2);

        // assertNotNull(store);
    }

}