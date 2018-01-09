package io.mewbase.rest;

import io.mewbase.MewbaseTestBase;
import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;

import io.mewbase.cqrs.Command;
import io.mewbase.cqrs.CommandManager;
import io.mewbase.cqrs.QueryManager;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.response.ResponseBodyExtractionOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;

import java.util.stream.Collectors;
import java.util.stream.IntStream;


import static io.restassured.RestAssured.*;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.*;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


/**
 *
 */
@RunWith(VertxUnitRunner.class)
public class RESTAdaptorTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(RESTAdaptorTest.class);

    final EventSink TEST_EVENT_SINK = EventSink.instance();
    final EventSource TEST_EVENT_SOURCE = EventSource.instance();

    final String quote = "\"";
    final String key = "Key";
    final String value = "Value";
    final String quotedKey = quote + key + quote;
    final String quotedValue = quote + value + quote;


    final Lock sequential = new ReentrantLock();


    public RestServiceAdaptor setUpServer() {
        sequential.lock();
        RestAssured.baseURI = "http://localhost:8081";
        return RestServiceAdaptor.instance();
    }

    public void tearDownServer(RestServiceAdaptor serv) {
        serv.stop();
        sequential.unlock();
    }

    @Test
    public void testCreateEmptyAdaptor() throws InterruptedException {

        RestServiceAdaptor serv = setUpServer();
        assertNotNull(serv);
        serv.start();

        // test get non route
        RestAssured.given().when().get("/nonexistant").then().statusCode(404);

        tearDownServer(serv);
    }



    @Test
    public void testGetDocumentById() throws Exception {

        final String docId = "Document-1234";

        // write a document into the store for the REST interface to find
        BinderStore store = BinderStore.instance(createConfig());
        final String testBinderName = new Object(){}.getClass().getEnclosingMethod().getName();
        Binder binder = store.open(testBinderName);
        BsonObject doc = new BsonObject().put(key, value);
        binder.put(docId,doc).join();

        // Create and configure the adaptor
        RestServiceAdaptor serv = setUpServer();
        serv.exposeGetDocument(store);
        serv.start();

        // positive
        Response resp =  RestAssured.
                given().
                when().
                    get("/binders/"+testBinderName+"/"+docId).
                then().
                    extract().response();
        System.out.println(resp.asString());

        RestAssured.
            given().
            when().
                get("/binders/"+testBinderName+"/"+docId).
            then().
                statusCode(200).
                body( key, is(value));

        // negative
        final String failDocName = "nonExistentDoc";
        RestAssured.
                given().
                when().
                get("/binders/"+testBinderName+"/"+failDocName).
                then().
                statusCode(500).
                statusLine( containsString(failDocName)).
                body( isEmptyOrNullString() );

        tearDownServer(serv);
    }


    @Test
    public void testListMultipleBindersAndDocs() throws Exception {

        final String binderPrefix = "Binder";
        final String docPrefix = "Document";


        BinderStore store = BinderStore.instance(createConfig());

        // write a set of binders with a document in each into the store for the REST interface to find
        final Set<String> expectedBinders = IntStream.rangeClosed(0,9).mapToObj( i  -> {
            final String binderName = binderPrefix +  i;
            final String docName = docPrefix + i;
            final Binder binder = store.open(binderName);
            BsonObject doc = new BsonObject().put(key,docName);
            binder.put(docName,doc).join();
            return binderName;
        }).collect(Collectors.toSet());

        // Create and configure the adaptor
        RestServiceAdaptor serv = setUpServer();
        serv.exposeGetDocument(store);
        serv.start();

        // List all binders in store
        final ResponseBodyExtractionOptions bindersJson = RestAssured.
                given().
                when().
                get("/binders").
                then().
                statusCode(200).
                extract().body();

        final List<String> bindersList = Arrays.asList(bindersJson.as(String[].class));
        expectedBinders.forEach( binderName -> assertTrue(bindersList.contains(binderName)));

        // List docs in a binder
        final int index = 3;
        final String targetBinder = binderPrefix + index;
        final String targetDoc = docPrefix + index;
        final ResponseBodyExtractionOptions docsJson = RestAssured.
                given().
                when().
                get("/binders" + "/" + targetBinder).
                then().
                statusCode(200).
                extract().body();

        final List<String> docsList = Arrays.asList(docsJson.as(String[].class));
        Collections.singleton(targetDoc).forEach(docName -> assertTrue(docsList.contains(docName)));

        tearDownServer(serv);
    }


     @Test
    public void testSimpleCommand(TestContext testContext) throws Exception {

         final String CHANNEL_NAME = "CommandTestChannel";
         final String COMMAND_NAME = "TestCommand";

        final CommandManager mgr = CommandManager.instance(TEST_EVENT_SINK);

        mgr.commandBuilder().
                    named(COMMAND_NAME).
                    emittingTo(CHANNEL_NAME).
                    // send the input JSON as the event BSON
                    as( (context -> {
                        // has the incoming Json been made into Bson correctly
                        assertNotNull(context);
                        BsonObject body = context.getBsonObject("body");
                        assertNotNull(body);
                        assertEquals(body.getString(key), value);
                        // path params are empty in this case.
                        BsonObject pathParams = context.getBsonObject("pathParams");
                        assertNotNull(pathParams);
                        return body;
                    }) ).
                    create();

        // Create and configure the adaptor
        RestServiceAdaptor serv = setUpServer();
        serv.exposeCommand(mgr,COMMAND_NAME);
        serv.start();

        // listen for an event arriving on the channel
         final CountDownLatch latch = new CountDownLatch(1);
         Subscription subs = TEST_EVENT_SOURCE.subscribe(CHANNEL_NAME, event ->  {
                     BsonObject bson  = event.getBson();
                     assertNotNull(bson);
                     assertEquals(bson.getString(key),value);
                     latch.countDown();
                 }
         );

         // post to the command
         final String inputJson = "{ "+quotedKey+" : "+quotedValue+" }";
         RestAssured.
                 given().
                    contentType("application/json").
                    and().
                    body(inputJson).
                 when().
                 post("/" + COMMAND_NAME).
                 then().
                 statusCode(200).
                 body( isEmptyOrNullString() );

         // latch clears when event received
         latch.await();

         tearDownServer(serv);
     }



    @Test
    public void testSimpleQuery(TestContext testContext) throws Exception {

        final String docId = "Document-";
        final String QUERY_NAME = "TestQuery";

        // write a document into the store for the REST interface to find
        BinderStore store = BinderStore.instance(createConfig());
        final String testBinderName = new Object(){}.getClass().getEnclosingMethod().getName();
        Binder binder = store.open(testBinderName);
        BsonObject doc = new BsonObject().put(key, value);
        IntStream.range(0,9).forEachOrdered(i -> binder.put(docId+i,doc));

        QueryManager qmgr = QueryManager.instance(store);

        BiPredicate<BsonObject, KeyVal<String, BsonObject>> allGoodFilter = (ctx, kv) -> true;

        qmgr.queryBuilder().
                named(QUERY_NAME).
                from(testBinderName).
                filteredBy(allGoodFilter).
                create();

        // Create and configure the adaptor
        RestServiceAdaptor serv = setUpServer();
        serv.exposeQuery(qmgr,QUERY_NAME);
        serv.start();

        RestAssured.
                given().
                    contentType("application/json").
                when().
                    get("/" + QUERY_NAME).
                then().
                    statusCode(200).
                    // sample in the set
                    body( "Document-4.Key", is(value) );

        tearDownServer(serv);
    }


}
