package io.mewbase.projection;

import io.mewbase.MewbaseTestBase;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;

import io.mewbase.binders.impl.filestore.FileBinder;
import io.mewbase.binders.impl.filestore.FileBinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSource;


import io.mewbase.projection.impl.ProjectionManagerImpl;
import io.mewbase.server.MewbaseOptions;
import io.vertx.ext.unit.junit.Repeat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
    private static final String TEST_BINDER = "ProjectionTestBinder";
    private static final String TEST_PROJECTION_NAME = "TestProjection";

    private static final String BASKET_ID_FIELD = "BasketID";

    private BinderStore store = null;
    private EventSource source = null;

    @Before
    public void before() throws Exception {
        store = BinderStore.instance(createMewbaseOptions());
        source = new NatsEventSource();
    }

    @After
    public void after() throws Exception {
        source.close();
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

        ProjectionManager factory = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        Projection projection = createProjection(builder, BASKET_ID_FIELD,  TEST_PROJECTION_NAME);

        assertNotNull(projection);
        assertEquals(TEST_PROJECTION_NAME, projection.getName());

        projection.stop();
    }


    @Test
    // @Repeat(50)
    public void testSimpleProjectionRuns() throws Exception {

        ProjectionManager manager = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = manager.builder();


        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(27);

        final CountDownLatch latch = new CountDownLatch(1);

        Projection projection = builder
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

        // Send an event to the channel which the projection is subscribed to.
        EventSink sink = new NatsEventSink();
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publish(TEST_CHANNEL, evt);

        latch.await();
        Thread.sleep(100);

        // try to recover the new document
        Binder binder = store.open(TEST_BINDER);
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(basketDoc);
        assertEquals(RESULT,basketDoc.getInteger("output"));

        projection.stop();
    }


    @Test
    public void testProjectionNames() throws Exception {

        ProjectionManager mgr = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = mgr.builder();

        Stream<String> names = IntStream.range(1,10).mapToObj( i -> {
            final String projName = "Proj" + i;
            createProjection(builder,BASKET_ID_FIELD,projName);
            return projName;
        });

        assertTrue( names.allMatch( name -> mgr.isProjection(name) ) );
    }


    @Test
    public void testProjectionRecoversFromEventNumber() throws Exception {

        ProjectionManager factory = ProjectionManager.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(27);

        final CountDownLatch latch = new CountDownLatch(1);

        final String MULTI_EVENT_CHANNEL = "MultiEventChannel";

        Projection projection = builder
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

        // Send an event to the channel which the projection is subscribed to.
        EventSink sink = new NatsEventSink();
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publish(MULTI_EVENT_CHANNEL, evt);

        latch.await();
        Thread.sleep(100);

        // Recover the new document
        Binder binder = store.open(TEST_BINDER);
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(basketDoc);
        assertEquals(RESULT,basketDoc.getInteger("output"));

        projection.stop();

        // binder now has offset event and valid current document
        // rebuild everything as tho' we had restarted.
        ProjectionManager newFactory = ProjectionManager.instance(source,store);
        ProjectionBuilder newBuilder = newFactory.builder();

        final CountDownLatch newLatch = new CountDownLatch(1);

        Projection newProjection = newBuilder
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

        // send another event on the same channel
        sink.publish(MULTI_EVENT_CHANNEL, evt);
        // and wait for the result
        newLatch.await();
        Thread.sleep(100);

        // Recover the new document
        BsonObject newBasketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(newBasketDoc);
        assertEquals(RESULT+RESULT,(long)newBasketDoc.getInteger("output"));

        newProjection.stop();
    }


    @Test
    public void testPartialWriteFailsStatefullyCorrect() throws Exception {

        // Patch the Binder so that fails in the most possible nasty way
        class FailingBinder extends FileBinder {
            public FailingBinder(String name, File binderDir) {
                super(name, binderDir);
            }
            @Override
            public CompletableFuture<Void> put(final String id, final BsonObject doc) {
                CompletableFuture fut = new CompletableFuture();
                fut.completeExceptionally(new Exception("partial failure nightmare"));
                return fut;
            }
        }

        class PartiallyFailingStore extends FileBinderStore {
            public PartiallyFailingStore(MewbaseOptions options) {
                super(options);
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

        BinderStore failingStore  = new PartiallyFailingStore(createMewbaseOptions());

        // copies the code exactly from a previous succeeding test.
        ProjectionManager manager = ProjectionManager.instance(source,failingStore);
        ProjectionBuilder builder = manager.builder();

        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(27);

        final CountDownLatch latch = new CountDownLatch(1);

        Projection projection = builder
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

        // Send an event to the channel which the projection is subscribed to.
        EventSink sink = new NatsEventSink();
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publish(TEST_CHANNEL, evt);

        latch.await();
        Thread.sleep(1000);

        // Try to recover the new document which was never written
        Binder binder = store.open(TEST_BINDER);
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNull(basketDoc);
        // check the event number was rewound
        Binder stateBinder = store.open(ProjectionManagerImpl.PROJ_STATE_BINDER_NAME);
        BsonObject state = stateBinder.get(TEST_PROJECTION_NAME).get();
        //assertEquals(0L , (long)state.getLong(ProjectionManagerImpl.EVENT_NUM_FIELD));
    }


    private Projection createProjection(ProjectionBuilder builder, String binderIdKey, String projName) {

        return builder
                .named(projName)
                .projecting(TEST_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(binderIdKey))
                .as( (basket, event) -> event.getBson().put("output",projName) )
                .create();
    }

}
