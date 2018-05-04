package io.mewbase.projection;

import com.typesafe.config.Config;
import io.mewbase.MewbaseTestBase;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;

import io.mewbase.binders.impl.filestore.FileBinder;
import io.mewbase.binders.impl.filestore.FileBinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;

import io.mewbase.eventsource.Subscription;
import io.mewbase.projection.impl.ProjectionManagerImpl;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * Created by tim on 30/09/16.
 */

public class ProjectionTest extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(ProjectionTest.class);

    private static final String TEST_CHANNEL = "ProjectionTestChannel";

    private static final String TEST_PROJECTION_NAME = "TestProjection";

    private static final String BASKET_ID_FIELD = "BasketID";



    private BinderStore store = null;
    private EventSource source = null;
    private EventSink sink = null;
    private Config cfg = null;

    @Before
    public void before() throws Exception {
        cfg = createConfig();
        store = BinderStore.instance(cfg);
        source = EventSource.instance(cfg);
        sink = EventSink.instance(cfg);
    }

    @After
    public void after() throws Exception {
        source.close();
        sink.close();
    }


    @Test
    public void testProjectionFactory() throws Exception {
        ProjectionManager factory = ProjectionManager.instance(source,store);
        assertNotNull(factory);
        ProjectionBuilder builder = factory.builder();
        assertNotNull(builder);
    }


    @Test
    public void testProjectionBuilder() throws Exception {

        final String TEST_BINDER = new Object(){}.getClass().getEnclosingMethod().getName();


        ProjectionManager factory = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        Projection projection = createProjection(builder,TEST_BINDER, BASKET_ID_FIELD,  TEST_PROJECTION_NAME);

        assertNotNull(projection);
        assertEquals(TEST_PROJECTION_NAME, projection.getName());

        projection.stop();
    }


    @Test
    // @Repeat(50)
    public void testSimpleProjectionRuns() throws Exception {

        final String TEST_BINDER = new Object(){}.getClass().getEnclosingMethod().getName();

        ProjectionManager manager = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = manager.builder();

        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(27);

        final CountDownLatch latch = new CountDownLatch(1);

        CompletableFuture<Projection> projectionFut = builder
                .named(TEST_PROJECTION_NAME)
                .projecting(TEST_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    assertNotNull(basket);
                    assertNotNull(event);
                    BsonObject out = event.getBson().put("output",RESULT);
                    latch.countDown();
                    return out;
                })
                .create();

        // ensure the projection is set up or times out
        final Projection projection = projectionFut.get(PROJECTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        // Send an event to the channel which the projection is subscribed to.
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publishSync(TEST_CHANNEL, evt);

        latch.await();
        Thread.sleep(200);

        // try to recover the new document
        Binder binder = store.open(TEST_BINDER);
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(basketDoc);
        assertEquals(RESULT,basketDoc.getInteger("output"));

        projection.stop();
    }


    @Test
    public void testProjectionNames() throws Exception {

        final String TEST_BINDER = new Object(){}.getClass().getEnclosingMethod().getName();

        ProjectionManager mgr = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = mgr.builder();

        Stream<String> names = IntStream.range(1,10).mapToObj( i -> {
            final String projName = "Proj" + i;
            final Projection projection = createProjection(builder, TEST_BINDER, BASKET_ID_FIELD, projName);
            return projName;
        });


        assertTrue( names.allMatch( name -> mgr.isProjection(name) ) );
    }


    @Test
    public void testProjectionStopsOnStop() throws Exception {

        final String TEST_BINDER = new Object(){}.getClass().getEnclosingMethod().getName();

        ProjectionManager factory = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        final String TEST_BASKET_ID = "TestBasket";

        final CountDownLatch latch = new CountDownLatch(1);

        final String MULTI_EVENT_CHANNEL = "MultiEventChannel";

        CompletableFuture<Projection> projectionFut =  builder
                .named(TEST_PROJECTION_NAME)
                .projecting(MULTI_EVENT_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    int visit = event.getBson().getInteger("counter",0);
                    BsonObject out = event.getBson().put("output", visit+1);
                    latch.countDown();
                    return out;
                })
                .create();

        // ensure the projection is set up or times out
        final Projection projection = projectionFut.get(PROJECTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        // Send an event to the channel which the projection is subscribed to.
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publishSync(MULTI_EVENT_CHANNEL, evt);

        latch.await();
        Thread.sleep(200);  // let the projextion fire off the event

        Binder binder = store.open(TEST_BINDER);
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(basketDoc);
        assertEquals(1,(long)basketDoc.getInteger("output"));

        projection.stop();
        Thread.sleep(200);  // let the projection stop.

        BsonObject evt2 = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publishSync(MULTI_EVENT_CHANNEL, evt2);
        Thread.sleep(200);  // let the event write and allow time to be processed (which should not happen)

        // ensure that the above event hasnt triggered another write.
        BsonObject sameBasketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(sameBasketDoc);
        assertEquals(1,(long)sameBasketDoc.getInteger("output"));
    }


    @Test
    public void testProjectionRecoversFromEventNumber() throws Exception {

        final String TEST_BINDER = new Object(){}.getClass().getEnclosingMethod().getName();

        ProjectionManager factory = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(1);

        final CountDownLatch latch = new CountDownLatch(1);

        final String MULTI_EVENT_CHANNEL = "MultiEventChannel";

        CompletableFuture<Projection> firstProjectionFut = builder
                .named(TEST_PROJECTION_NAME)
                .projecting(MULTI_EVENT_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    BsonObject out = event.getBson().put("output",RESULT);
                    latch.countDown();
                    return out;
                })
                .create();

        // ensure the projection is set up or times out
        final Projection firstProjection = firstProjectionFut.get(PROJECTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        // Send an event to the channel which the projection is subscribed to.
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publishSync(MULTI_EVENT_CHANNEL, evt);

        latch.await();

        Thread.sleep(100);

        // Recover the new document
        Binder binder = store.open(TEST_BINDER);
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(basketDoc);
        assertEquals(RESULT,basketDoc.getInteger("output"));

        firstProjection.stop();
        Thread.sleep(100);

        // binder now has offset event and valid current document
        // rebuild everything as though we had restarted.
        source = EventSource.instance(cfg);
        sink = EventSink.instance(cfg);
        ProjectionManager newFactory = ProjectionManager.instance(source,store);

        final CountDownLatch newLatch = new CountDownLatch(1);

        CompletableFuture<Projection> secondProjectionFut = newFactory.builder()
                .named(TEST_PROJECTION_NAME)
                .projecting(MULTI_EVENT_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    final int currentVal = basket.getInteger("output");
                    basket.put("output",RESULT+currentVal);
                    newLatch.countDown();
                    return basket;
                })
                .create();

        // ensure the projection is set up or times out
        final Projection secondProjection = secondProjectionFut.get(PROJECTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        // send another event on the same channel
        sink.publishSync(MULTI_EVENT_CHANNEL, evt);

        newLatch.await();

        Thread.sleep(100);

        // Recover the new document
        CompletableFuture<BsonObject> f = binder.get(TEST_BASKET_ID);
        BsonObject newBasketDoc = f.get();
        assertNotNull(newBasketDoc);
        assertEquals(RESULT+RESULT,(long)newBasketDoc.getInteger("output"));

        secondProjection.stop();
    }


    @Test
    public void testPartialWriteFailsStatefullyCorrect() throws Exception {

        final String TEST_BINDER = new Object(){}.getClass().getEnclosingMethod().getName();

        // Patch the Binder so that fails in the most possible nasty way
        class FailingBinder extends FileBinder {
            public FailingBinder(String name, File binderDir) {
                super(name, binderDir);
            }
            @Override
            public CompletableFuture<Void> put(final String id, final BsonObject doc) {
                CompletableFuture<Void> fut = new CompletableFuture<Void>();
                fut.completeExceptionally(new Exception("partial failure nightmare"));
                return fut;
            }
        }

        class PartiallyFailingStore extends FileBinderStore {
            public PartiallyFailingStore(Config cfg) {
                super(cfg);
            }
            @Override
            public Binder open(String name) {
                // poison the basket write fail
                if (name.equals(TEST_BINDER))
                    return binders.computeIfAbsent(name, k -> new FailingBinder(k, new File(bindersDir,name)));
                else
                    return super.open(name);
            }
        }

        BinderStore failingStore  = new PartiallyFailingStore(createConfig());

        final String UNIQUE_PROJECTION_NAME = TEST_PROJECTION_NAME + UUID.randomUUID();
        final String UNIQUE_CHANNEL_NAME = TEST_CHANNEL + UUID.randomUUID();

        // copies the code exactly from a previous succeeding test.
        ProjectionManager manager = ProjectionManager.instance(source,failingStore);

        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(27);

        final CountDownLatch latch = new CountDownLatch(1);

        CompletableFuture<Projection> projectionFut  = manager.builder()
                .named(UNIQUE_PROJECTION_NAME)
                .projecting(UNIQUE_CHANNEL_NAME)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    assertNotNull(basket);
                    assertNotNull(event);
                    BsonObject out = event.getBson().put("output",RESULT);
                    latch.countDown();
                    return out;
                })
                .create();

        Projection proj = projectionFut.get(PROJECTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);
        // Send an event to the channel which the projection is subscribed to.
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publishSync(UNIQUE_CHANNEL_NAME, evt);

        latch.await();
        Thread.sleep(200);

        // Try to recover the new document which should not have been written
        Binder binder = store.open(TEST_BINDER);
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNull(basketDoc);

        // Try to recover the state update that wasnt written
        Binder stateBinder = store.open(ProjectionManagerImpl.PROJ_STATE_BINDER_NAME);
        BsonObject state = stateBinder.get(UNIQUE_PROJECTION_NAME).get();
        assertNull(state);
        proj.stop();
    }


    private Projection createProjection(ProjectionBuilder builder, String testBinder, String binderIdKey, String projName) {

        CompletableFuture<Projection> projectionFut = builder
                .named(projName)
                .projecting(TEST_CHANNEL)
                .onto(testBinder)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(binderIdKey))
                .as( (basket, event) -> event.getBson().put("output",projName) )
                .create();
        try {
            return projectionFut.get(PROJECTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);
        } catch(Exception exp) {
            throw new IllegalStateException(exp);
        }

    }

}
