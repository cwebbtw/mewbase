package example.json2bson;


import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.vertx.core.json.JsonObject;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;


/*
 * Simple example of converting arbitrarily large Json docs into Bson docs and
 * sending them over an EventSink to an EventSource and converting the response
 * back to Json
 *
 * Created  Nige on 18/4/18
 */

public class Json2BsonExample {

    public static void main(String[] args) throws Exception {

        final String CHANNEL_NAME = "Json2BsonChannel";
        final int NUMBER_OF_JSON_BLOCKS = 128;  // choose your size

        final String json = createJson(NUMBER_OF_JSON_BLOCKS);

        // make a JsonObject from the String could one line this
        JsonObject jobj = new JsonObject(json);
        BsonObject bobj = new BsonObject(jobj);

        // compare encoding size just FYI not necessary
        System.out.println("Size of Json :"+json.length()+" Size of Bson :"+bobj.encode().length());

        // set up a sink
        EventSink sink = EventSink.instance();
        // and send the Event waiting for the write to confirm
        sink.publishSync(CHANNEL_NAME, bobj);

        // Set up the event source
        EventSource src = EventSource.instance();
        // Use a latch to wait for the result
        CountDownLatch cdl = new CountDownLatch(1);
        // Grab the event from the top of the stream and convert it to a json String
        src.subscribeFromMostRecent(CHANNEL_NAME, event -> {
            String result = event.getBson().encodeToString();
            // check it is equivalent to the original string
            System.out.println("Equivalent : "+ areEquivalentJson(json,result));
            cdl.countDown();
        });

        cdl.await();

        // close the resources
        sink.close();
        src.close();
    }


    private static Boolean areEquivalentJson(String js1, String js2) {
        JsonObject jo1 = new JsonObject(js1);
        JsonObject jo2 = new JsonObject(js2);
        return jo1.equals(jo2);
    }


    private static String createJson( int n) {
        StringBuilder sb = new StringBuilder();
        sb.append("{ \n");
        IntStream.range(1,n).forEach( i -> sb.append( " \"Field"+i+"\" : "+i+",\n"));
        sb.append(" \"Goodnight\" : \"Vienna\" \n" );
        sb.append("}");
        return sb.toString();
    }


}
