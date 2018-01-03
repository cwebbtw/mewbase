package io.mewbase.rest;

import io.mewbase.MewbaseTestBase;
import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;

import io.restassured.RestAssured;
import io.restassured.response.ResponseBodyExtractionOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
    // json loves a quote
    private final static String quote = "\"";


    @Test
    public void testCreateEmptyAdaptor() throws InterruptedException {

        RestServiceAdaptor serv = RestServiceAdaptor.instance();
        assertNotNull(serv);
        serv.start();

        // rest assured DSL attempt to get nothing
        given().when().get("/nonexistant").then().statusCode(404);

        serv.stop();
    }



    @Test
    public void testSingleFindById() throws Exception {

        final String docId = "Document-1234";
        final String key = "Key";
        final String value = "Value";

        // write a document into the store for the REST interface to find
        BinderStore store = BinderStore.instance(createConfig());
        final String testBinderName = new Object(){}.getClass().getEnclosingMethod().getName();
        Binder binder = store.open(testBinderName);
        BsonObject doc = new BsonObject().put(key, value);
        binder.put(docId,doc);

        // Create and configure the adaptor
        RestServiceAdaptor serv = RestServiceAdaptor.instance();
        serv.exposeGetDocument(store);
        serv.start();

        final String quote = "\"";
        final String jsonKey = quote + key + quote;

        RestAssured.
            given().
            when().
                get("/binders/"+testBinderName+"/"+docId).
            then().
                statusCode(200).
                body( jsonKey, is(value));

        serv.stop();
    }


    @Test
    public void testListMultipleBindersAndDocs() throws Exception {

        final String binderPrefix = "Binder";
        final String docPrefix = "Document";
        final String key = "Key";

        BinderStore store = BinderStore.instance(createConfig());

        // write a set of binders with a document in each into the store
        // for the REST interface to find
        final Set<String> expected = IntStream.rangeClosed(0,9).mapToObj( i  -> {
            final String binderName = binderPrefix +  i;
            final String docName = docPrefix + i;
            final Binder binder = store.open(binderName);
            BsonObject doc = new BsonObject().put(key,docName);
            binder.put(docName,doc);
            return binderName;
        }).collect(Collectors.toSet());


        // Create and configure the adaptor
        RestServiceAdaptor serv = RestServiceAdaptor.instance();
        serv.exposeGetDocument(store);
        serv.start();

        final ResponseBodyExtractionOptions body = RestAssured.
                given().
                when().
                get("/binders").
                then().
                statusCode(200).
                extract().body();

        final List<String> results = Arrays.asList(body.as(String[].class));
        expected.forEach( binderName -> assertTrue(results.contains(binderName)));

        serv.stop();
    }

    


//    @Test
    public void testSimpleCommand(TestContext testContext) throws Exception {
        String commandName = "testcommand";
//        CommandHandler handler = server.buildCommandHandler(commandName)
////                .emittingTo(TEST_CHANNEL_1)
////                .as((command, context) -> {
////                    testContext.assertNull(command.getBsonObject("pathParams"));
////                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
////                    context.complete();
////                })
//                .create();
//
//        assertNotNull(handler);
//        assertEquals(commandName, handler.getName());

  //      Async async = testContext.async(2);

//        Consumer<ClientDelivery> subHandler = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals("foobar", event.getString("eventField"));
//            async.complete();
//        };

        //client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1), subHandler).get();

//        server.exposeCommand(commandName, "/orders", HttpMethod.POST);

        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

//        HttpClient httpClient = vertx.createHttpClient();
//        HttpClientRequest req = httpClient.request(HttpMethod.POST, 8080, "localhost", "/orders", resp -> {
//            assertEquals(200, resp.statusCode());
//           // async.complete();
//        });
//        req.putHeader("content-type", "text/json");
//        req.end(sentCommand.encode());
    }

    //@Test
    public void testSimpleQuery(TestContext testContext) throws Exception {

        String queryName = "testQuery";

        int numDocs = 100;
        BsonArray bsonArray = new BsonArray();
        for (int i = 0; i < numDocs; i++) {
            String docID = getIdAsString(i);
            BsonObject doc = new BsonObject().put("id", docID).put("foo", "bar");
           // prod.publish(doc).get();
            bsonArray.add(doc);
        }

       // waitForDoc(numDocs - 1);

        // Setup a query
//        server.buildQuery(queryName).documentFilter((doc, ctx) -> {
//            return true;
//        }).from(TEST_BINDER1).create();

//        server.exposeQuery(queryName, "/orders/");

        Async async = testContext.async();

//        HttpClient httpClient = vertx.createHttpClient();
//        HttpClientRequest req = httpClient.request(HttpMethod.GET, 8080, "localhost", "/orders/", resp -> {
//            assertEquals(200, resp.statusCode());
//            resp.bodyHandler(body -> {
//                BsonArray arr = new BsonArray(new JsonArray(body.toString()));
//                assertEquals(bsonArray, arr);
//                async.complete();
//            });
//            resp.exceptionHandler(t -> t.printStackTrace());
//        });
//        req.exceptionHandler(t -> {
//            t.printStackTrace();
//        });

//        req.putHeader("content-type", "text/json");
//        req.end();

    }




    protected void installInsertProjection() {
//        server.buildProjection("testproj").projecting(TEST_CHANNEL_1).onto(TEST_BINDER1).filteredBy(ev -> true)
//                .identifiedBy(ev -> ev.getString("id"))
//                .as((basket, del) -> del.event()).create();
    }


    protected String getIdAsString(int id) {
        return String.format("id-%05d", id);
    }


}
