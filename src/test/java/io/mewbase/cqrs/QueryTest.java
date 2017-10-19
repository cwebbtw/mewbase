package io.mewbase.cqrs;

import io.mewbase.ServerTestBase;
import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonObject;


import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;


import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Predicate;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by Jamie on 14/10/2016.
 */
@RunWith(VertxUnitRunner.class)
public class QueryTest extends ServerTestBase {

    final BinderStore TEST_BINDER_STORE = new LmdbBinderStore();
    final String TEST_BINDER_NAME = "TestBinder";
    final Binder TEST_BINDER  = TEST_BINDER_STORE.open(TEST_BINDER_NAME);

    final String TEST_QUERY_NAME = "TestQuery";
    final String TEST_DOCUMENT_ID = "TestId";


    @Test
    public void testQueryManager() throws Exception {

        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);
        assertNotNull(mgr);
        final String QUERY_NAME = "NotAQuery";
        assertNotNull(mgr.queryBuilder());
        assertEquals(0, mgr.getQueries().count()); // no commands registered
        // TODO
//        CompletableFuture<BsonObject> futEvt = mgr.execute(QUERY_NAME, new BsonObject() );
//        futEvt.handle( (good, bad) -> {
//            assertNull("Executing a non query should fail.", good);
//            assertNotNull("Executing a non query should not result in a doc", bad);
//            final String msg = bad.getMessage();
//            assertTrue(msg.contains(QUERY_NAME));
//            return null;
//        });
    }

    @Test
    public void testQueryBuilder() throws Exception {

        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);

        Predicate<BsonObject> identity = document -> true;

        mgr.queryBuilder().
                named(TEST_QUERY_NAME).
                from(TEST_BINDER_NAME).
                filteredBy(identity).
                create();

        Optional<Query> qOpt = mgr.getQueries().findFirst();
        assert(qOpt.isPresent());
        Query query = qOpt.get();
        assertEquals(query.getName(),TEST_QUERY_NAME);
        assertEquals(query.getBinder(),TEST_BINDER);
        assertEquals(query.getDocumentFilter(),identity);
        assertNull(query.getIdSelector());
    }


    @Test(expected = NoSuchElementException.class)
    public void testNoSuchBinder() throws Exception {

        final String NON_EXISTENT_BINDER = "Junk";
        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);

        Predicate<BsonObject> identity = document -> true;

        mgr.queryBuilder().
                named(TEST_QUERY_NAME).
                from(NON_EXISTENT_BINDER).
                filteredBy(identity).
                create();
    }


    @Test
    public void testFiltered() throws Exception {

        // Set up the binder
        BsonObject doc = new BsonObject().put("Val", "Doc");
        TEST_BINDER.put(TEST_DOCUMENT_ID, doc);

        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);

        Predicate<BsonObject> identity = document -> true;

        mgr.queryBuilder().
                named(TEST_QUERY_NAME).
                from(TEST_BINDER_NAME).
                filteredBy(identity).
                create();


        Optional<Query> qOpt = mgr.getQueries().findFirst();
        assert (qOpt.isPresent());
        Query query = qOpt.get();
        assertEquals(query.getName(), TEST_QUERY_NAME);
        assertEquals(query.getBinder(), TEST_BINDER);
        assertEquals(query.getDocumentFilter(), identity);
        assertNull(query.getIdSelector());

//    assertEquals(docID, received.getString("id"));
//   assertEquals("bar", received.getString("foo"));
    }



    @Test
    public void testId() throws Exception {
        final int TOTAL_DOCS = 100;
//        for (int i = 0; i < numDocs; i++) {
            //String docID = getID(i);
            //BsonObject doc = new BsonObject().put("id", docID).put("foo", "bar");
         //   prod.publish(doc).get();
        }

       // waitForDoc(numDocs - 1);

        // Setup a query
//        server.buildQuery("testQuery").documentFilter((doc, ctx) -> {
//            return true;
//        }).from(TEST_BINDER1).create();

//        Async async = context.async();
//        AtomicInteger cnt = new AtomicInteger();
//        client.executeQuery("testQuery", new BsonObject(), qr -> {
//            String expectedID = getID(cnt.getAndIncrement());
//            context.assertEquals(expectedID, qr.document().getString("id"));
//            if (cnt.get() == numDocs) {
//                context.assertTrue(qr.isLast());
//                async.complete();
//            } else {
//                context.assertFalse(qr.isLast());
//            }
//        }, t -> context.fail("Exception shouldn't be received"));
//


}
