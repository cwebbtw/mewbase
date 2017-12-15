package io.mewbase;


import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;

/**
 * Created by tim on 01/01/17.
 */
public class ServerTestBase extends MewbaseTestBase {

    protected Vertx vertx;

    @Before
    public void before(TestContext context) throws Exception {
        setup(context);
    }

    @After
    public void after(TestContext context) throws Exception {
        tearDown(context);
    }

    protected void setup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
    }

    protected void tearDown(TestContext context) throws Exception {

    }

    protected void startServer() throws Exception {

    }

    protected void stopServer() throws Exception {

    }

    protected void restart() throws Exception {

    }


}