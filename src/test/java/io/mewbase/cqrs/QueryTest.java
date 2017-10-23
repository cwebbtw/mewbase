package io.mewbase.cqrs;

import io.mewbase.MewbaseTestBase;
import io.mewbase.ServerTestBase;
import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonObject;


import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.*;

/**
 * Created by Jamie on 14/10/2016.
 */
@RunWith(VertxUnitRunner.class)
public class QueryTest extends MewbaseTestBase {

    final String TEST_BINDER_NAME = "TestBinder";

    final String TEST_QUERY_NAME = "TestQuery";
    final String TEST_DOCUMENT_ID = "TestId";


    @Test
    public void testQueryManager() throws Exception {
        final BinderStore TEST_BINDER_STORE = new LmdbBinderStore(createMewbaseOptions());

        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);
        assertNotNull(mgr);
        assertNotNull(mgr.queryBuilder());
        assertEquals(0, mgr.getQueries().count()); // no commands registered
    }

    @Test
    public void testQueryBuilder() throws Exception {

        final BinderStore TEST_BINDER_STORE = new LmdbBinderStore(createMewbaseOptions());
        final Binder TEST_BINDER  = TEST_BINDER_STORE.open(TEST_BINDER_NAME);
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


    /* Testing for exception due to a reference to an uncreated binder */
    @Test(expected = NoSuchElementException.class)
    public void testNoSuchBinder() throws Exception {

        final BinderStore TEST_BINDER_STORE = new LmdbBinderStore(createMewbaseOptions());

        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);

        final String NON_EXISTENT_BINDER = "Junk";
        Predicate<BsonObject> identity = document -> true;

        mgr.queryBuilder().
                named(TEST_QUERY_NAME).
                from(NON_EXISTENT_BINDER).
                filteredBy(identity).
                create();
    }


    @Test
    public void testFiltered() throws Exception {

        final String KEY_TO_MATCH = "Key";
        final long VAL_TO_MATCH = 27;

        final String DOC_ID_NOT_TO_MATCH = "NoMatchingDocID";
        final long VAL_TO_NOT_MATCH = 34;

        // Set up the binder with a document
        final BinderStore TEST_BINDER_STORE = new LmdbBinderStore(createMewbaseOptions());
        final Binder TEST_BINDER  = TEST_BINDER_STORE.open(TEST_BINDER_NAME);

        // Matching record
        BsonObject doc = new BsonObject().put(KEY_TO_MATCH, VAL_TO_MATCH);
        TEST_BINDER.put(TEST_DOCUMENT_ID, doc);

        // Non Matching record
        BsonObject anotherDoc  = new BsonObject().put(KEY_TO_MATCH, VAL_TO_NOT_MATCH);
        TEST_BINDER.put(DOC_ID_NOT_TO_MATCH, anotherDoc);


        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);

        Predicate<BsonObject> filter = document -> document.getInteger(KEY_TO_MATCH) == VAL_TO_MATCH;

        mgr.queryBuilder().
                named(TEST_QUERY_NAME).
                from(TEST_BINDER_NAME).
                filteredBy(filter).
                create();

        BsonObject params = new BsonObject(); // no params for identity
        Stream<Query.Result> resultStream = mgr.execute(TEST_QUERY_NAME, params);

        Set<Query.Result> resultSet = resultStream.map(result -> {
                assertEquals(TEST_DOCUMENT_ID, result.getId());
                assertEquals((Long)VAL_TO_MATCH, result.getDocument().getLong(KEY_TO_MATCH));
                assertNotEquals(DOC_ID_NOT_TO_MATCH, result.getId());
                assertNotEquals((Long)VAL_TO_NOT_MATCH, result.getDocument().getLong(KEY_TO_MATCH));
                return result;
                }
            ).collect(Collectors.toSet());

        assertEquals(1, resultSet.size());
    }


    @Test
    public void testIdSelector() throws Exception {
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
