package io.mewbase.cqrs;

import io.mewbase.MewbaseTestBase;
import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;


import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;


import java.util.*;
import java.util.function.Function;
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

    final String KEY_TO_MATCH = "Key";
    final long VAL_TO_MATCH = 27;

    final String DOC_ID_NOT_TO_MATCH = "NoMatchingDocID";
    final long VAL_TO_NOT_MATCH = 34;


    @Test
    public void testQueryManager() throws Exception {
        final BinderStore TEST_BINDER_STORE = BinderStore.instance(createConfig());

        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);
        assertNotNull(mgr);
        assertNotNull(mgr.queryBuilder());
        assertEquals(0, mgr.getQueries().count()); // no commands registered
    }

    @Test
    public void testQueryBuilder() throws Exception {

        final BinderStore TEST_BINDER_STORE = BinderStore.instance(createConfig());
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

        final BinderStore TEST_BINDER_STORE = BinderStore.instance(createConfig());

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

        // Set up the binder
        final BinderStore TEST_BINDER_STORE = BinderStore.instance(createConfig());
        final Binder TEST_BINDER  = TEST_BINDER_STORE.open(TEST_BINDER_NAME);

        // Matching document
        BsonObject doc = new BsonObject().put(KEY_TO_MATCH, VAL_TO_MATCH);
        TEST_BINDER.put(TEST_DOCUMENT_ID, doc);

        // Non Matching document
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
        Stream<KeyVal<String,BsonObject>> resultStream = mgr.execute(TEST_QUERY_NAME, params);

        Set<KeyVal<String,BsonObject>> resultSet = resultStream.map(result -> {
                    assertEquals(TEST_DOCUMENT_ID, result.getKey());
                    assertEquals((Long)VAL_TO_MATCH, result.getValue().getLong(KEY_TO_MATCH));
                    assertNotEquals(DOC_ID_NOT_TO_MATCH, result.getKey());
                    assertNotEquals((Long)VAL_TO_NOT_MATCH, result.getValue().getLong(KEY_TO_MATCH));
                    return result;
                }
            ).collect(Collectors.toSet());

        assertEquals(1, resultSet.size());
    }


    @Test
    public void testIdSelector() throws Exception {
        // Set up the binder
        final BinderStore TEST_BINDER_STORE = BinderStore.instance(createConfig());
        final Binder TEST_BINDER = TEST_BINDER_STORE.open(TEST_BINDER_NAME);

        // Matching document
        BsonObject doc = new BsonObject().put(KEY_TO_MATCH, VAL_TO_MATCH);
        TEST_BINDER.put(TEST_DOCUMENT_ID, doc);

        // Non Matching document
        BsonObject anotherDoc = new BsonObject().put(KEY_TO_MATCH, VAL_TO_NOT_MATCH);
        TEST_BINDER.put(DOC_ID_NOT_TO_MATCH, anotherDoc);

        QueryManager mgr = QueryManager.instance(TEST_BINDER_STORE);

        Set<String> testId = new HashSet<String>();
        testId.add(TEST_DOCUMENT_ID);
        Function<BsonObject, Set<String>> selector = params -> (testId);

        mgr.queryBuilder().
                named(TEST_QUERY_NAME).
                from(TEST_BINDER_NAME).
                selectedBy(selector).
                create();

        BsonObject params = new BsonObject(); // no params for identity
        Stream<KeyVal<String, BsonObject>> resultStream = mgr.execute(TEST_QUERY_NAME, params);

        Set<KeyVal<String, BsonObject>> resultSet = resultStream.map(result -> {
                    assertEquals(TEST_DOCUMENT_ID, result.getKey());
                    assertEquals((Long) VAL_TO_MATCH, result.getValue().getLong(KEY_TO_MATCH));
                    assertNotEquals(DOC_ID_NOT_TO_MATCH, result.getKey());
                    assertNotEquals((Long) VAL_TO_NOT_MATCH, result.getValue().getLong(KEY_TO_MATCH));
                    return result;
                }
        ).collect(Collectors.toSet());

        assertEquals(1, resultSet.size());
    }

}
