package io.mewbase.metrics;



import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonValue;
import org.junit.Test;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * Created by Nige on 30/09/16.
 */

public class MetricsRegistryTest {

    @Test
    public void testSimpleRegistry()  {

        // set up the meters
        MetricsRegistry.ensureRegistry();

        // get the document from the registry
        final BsonObject metersDoc = MetricsRegistry.allMetricsAsDocument();

        final BsonArray meters = metersDoc.getBsonArray("meters");
        final Stream<BsonObject> metrsObjs = meters.getList().stream().map( m -> ((BsonValue.BsonObjectBsonValue)m).getValue());
        final Predicate<BsonObject> pred = (BsonObject meter) -> meter.getString("name").equals("jvm.classes.loaded");

        final List<BsonObject> classesLoaded = metrsObjs.filter(  pred ).collect(Collectors.toList());
        assertEquals( "Not only one JVM classes loaded meter", 1, classesLoaded.size() );
        BsonObject classesMeter = classesLoaded.get(0);
        BsonArray tags = classesMeter.getBsonArray("tags");
        assertEquals( "JVM classes loaded should not have tags ", 0, tags.size() );
        BsonArray measures = classesMeter.getBsonArray("measures");
        assertTrue( "No measurement for classes loaded", measures.getBsonObject(0).getDouble("VALUE") > 1);
    }



}



