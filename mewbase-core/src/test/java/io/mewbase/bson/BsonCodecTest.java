package io.mewbase.bson;

import io.mewbase.TestUtils;
import io.vertx.core.buffer.Buffer;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.time.Instant;

import static org.junit.Assert.*;

public class BsonCodecTest {

    @Test
    public void convertEmptyObject() {
        assertEquals(Json.createObjectBuilder().build(), BsonCodec.bsonObjectToJsonObject(new BsonObject()));
    }

    @Test
    public void convertObjectWithNullField() {
        assertEquals(Json.createObjectBuilder().addNull("woah").build(), BsonCodec.bsonObjectToJsonObject(new BsonObject().putNull("woah")));
    }

    @Test
    public void convertObjectWithStringField() {
        assertEquals(Json.createObjectBuilder().add("hello", "world").build(),
                BsonCodec.bsonObjectToJsonObject(new BsonObject().put("hello", "world")));
    }

    @Test
    public void convertObjectWithIntField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123).build(),
                BsonCodec.bsonObjectToJsonObject(new BsonObject().put("hello", 123)));
    }

    @Test
    public void convertObjectWithLongField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123).build(),
                BsonCodec.bsonObjectToJsonObject(new BsonObject().put("hello", 123L)));
    }

    @Test
    public void convertObjectWithDoubleField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123d).build(),
                BsonCodec.bsonObjectToJsonObject(new BsonObject().put("hello", 123.0d)));
    }

    @Test
    public void convertObjectWithFloatField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123f).build(),
                BsonCodec.bsonObjectToJsonObject(new BsonObject().put("hello", 123.0f)));
    }

    @Test
    public void convertObjectWithBooleanField() {
        assertEquals(Json.createObjectBuilder().add("hello", true).build(),
                BsonCodec.bsonObjectToJsonObject(new BsonObject().put("hello", true)));
    }

    @Test
    public void convertObjectWithObjectField() {
        final BsonObject innerBson = new BsonObject().put("mind", "blown");
        final BsonObject outerBson = new BsonObject().put("inception", innerBson);

        final JsonObjectBuilder innerJson = Json.createObjectBuilder().add("mind", "blown");
        final JsonObject outerJson = Json.createObjectBuilder().add("inception", innerJson).build();

        assertEquals(outerJson,
                BsonCodec.bsonObjectToJsonObject(outerBson));
    }

    @Test
    public void convertObjectWithArrayField() {
        final BsonObject bson = new BsonObject().put("mind", new BsonArray().add("blown"));
        final JsonObject json = Json.createObjectBuilder().add("mind", Json.createArrayBuilder().add("blown").build()).build();

        assertEquals(json,
                BsonCodec.bsonObjectToJsonObject(bson));
    }

    @Test
    public void convertEmptyArray() {
        assertEquals(Json.createArrayBuilder().build(), BsonCodec.bsonArrayToJsonArray(new BsonArray()));
    }

    @Test
    public void convertArrayWithNullElement() {
        assertEquals(Json.createArrayBuilder().addNull().build(), BsonCodec.bsonArrayToJsonArray(new BsonArray().addNull()));
    }

    @Test
    public void convertArrayWithStringElement() {
        assertEquals(Json.createArrayBuilder().add("hello").build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add("hello")));
    }

    @Test
    public void convertArrayWithIntElement() {
        assertEquals(Json.createArrayBuilder().add(123).build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add(123)));
    }

    @Test
    public void convertArrayWithLongElement() {
        assertEquals(Json.createArrayBuilder().add(123L).build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add(123L)));
    }

    @Test
    public void convertArrayWithDoubleElement() {
        assertEquals(Json.createArrayBuilder().add(123d).build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add(123d)));
    }

    @Test
    public void convertArrayWithFloatElement() {
        assertEquals(Json.createArrayBuilder().add(123f).build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add(123f)));
    }

    @Test
    public void convertArrayWithBooleanElement() {
        assertEquals(Json.createArrayBuilder().add(true).build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add(true)));
    }

    @Test
    public void convertArrayWithObjectElement() {
        final BsonObject bsonObject = new BsonObject().put("mind", "blown");
        final JsonObject jsonObject = Json.createObjectBuilder().add("mind", "blown").build();

        assertEquals(Json.createArrayBuilder().add(jsonObject).build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add(bsonObject)));
    }

    @Test
    public void convertArrayWithArrayElement() {
        final BsonArray innerBson = new BsonArray().add("mind").add("blown");
        final JsonArray innerJson = Json.createArrayBuilder().add("mind").add("blown").build();

        assertEquals(Json.createArrayBuilder().add(innerJson).build(),
                BsonCodec.bsonArrayToJsonArray(new BsonArray().add(innerBson)));
    }

    @Test
    public void jsonObjectBsonRoundTrip() {
        final JsonObject jsonObject =
                Json.createObjectBuilder().add("hello", "world").build();
        assertEquals(jsonObject, BsonCodec.bsonBytesToJsonObject(BsonCodec.jsonObjectToBsonBytes(jsonObject)));
    }

    @Test
    public void jsonArrayBsonRoundTrip() throws UnsupportedEncodingException {
        final JsonArray jsonArray =
                Json.createArrayBuilder().add(123).build();
        byte[] bytes = BsonCodec.jsonArrayToBsonBytes(jsonArray);
        System.out.println(new String(bytes, "UTF-8"));
        assertEquals(jsonArray, BsonCodec.bsonBytesToJsonArray(bytes));
    }

    @Test
    public void shouldInstantiateBsonObjectfromJsonObject() {
        final String key = "event_type";
        final String value = "manually_typed_address_merged";
        final String jsonStr = "{\""+key+"\":\""+value+"\"}";
        final StringReader stringReader = new StringReader(jsonStr);
        final javax.json.JsonObject jsonObject = javax.json.Json.createReader(stringReader).readObject();
        final BsonObject bsonObject = BsonCodec.jsonObjectToBsonObject(jsonObject);
        assertNotNull(bsonObject);
        assertEquals(bsonObject.getString(key),value);
    }

    @Test
    public void testEncode() {
        final BsonObject bsonObject = createBsonObject();
        bsonObject.put("mystr", "foo");
        bsonObject.put("mycharsequence", new StringBuilder("oob"));
        bsonObject.put("myint", 123);
        bsonObject.put("mylong", 1234l);
        bsonObject.put("myfloat", 1.23f);
        bsonObject.put("mydouble", 2.34d);
        bsonObject.put("myboolean", true);
        byte[] bytes = TestUtils.randomByteArray(10);
        bsonObject.put("mybinary", bytes);
        Instant now = Instant.now();
        bsonObject.put("myinstant", now);
        bsonObject.putNull("mynull");
        bsonObject.put("myobj", new BsonObject().put("foo", "bar"));
        bsonObject.put("myarr", new BsonArray().add("foo").add(123));
        byte[] encoded = BsonCodec.bsonObjectToBsonBytes(bsonObject);
        BsonObject obj = BsonCodec.bsonBytesToBsonObject(encoded);
        assertEquals("foo", obj.getString("mystr"));
        assertEquals("oob", obj.getString("mycharsequence"));
        assertEquals(Integer.valueOf(123), obj.getInteger("myint"));
        assertEquals(Long.valueOf(1234), obj.getLong("mylong"));
        assertEquals(Float.valueOf(1.23f), obj.getFloat("myfloat"));
        assertEquals(Double.valueOf(2.34d), obj.getDouble("mydouble"), 0.001);
        assertTrue(obj.getBoolean("myboolean"));
        assertTrue(TestUtils.byteArraysEqual(bytes, obj.getBinary("mybinary")));
        assertEquals(now, obj.getInstant("myinstant"));
        assertTrue(obj.containsKey("mynull"));
        BsonObject nestedObj = obj.getBsonObject("myobj");
        assertEquals("bar", nestedObj.getString("foo"));
        BsonArray nestedArr = obj.getBsonArray("myarr");
        assertEquals("foo", nestedArr.getString(0));
        assertEquals(Integer.valueOf(123), Integer.valueOf(nestedArr.getInteger(1)));
    }


    @Test
    public void testJsonEncoding() {
        final BsonObject bsonObject = createBsonObject();
        bsonObject.put("mystr", "foo");
        bsonObject.put("mycharsequence", new StringBuilder("oob"));
        bsonObject.put("mylong", 1234L);
        bsonObject.put("mydouble", 2.34d);
        bsonObject.put("myboolean", true);
        byte[] bytes = new byte[] {4, 7, 89, 32, 24};
        bsonObject.put("mybinary", bytes);
        Instant now = Instant.ofEpochMilli(16235126312635L);
        bsonObject.put("myinstant", now);
        bsonObject.putNull("mynull");
        bsonObject.put("myobj", new BsonObject().put("foo", "bar"));
        bsonObject.put("myarr", new BsonArray().add("foo").add(123L));

        final String jsonString = BsonCodec.bsonObjectToJsonObject(bsonObject).toString();
        final javax.json.JsonReader reader = javax.json.Json.createReader(new StringReader(jsonString));
        final BsonObject parsed = BsonCodec.jsonObjectToBsonObject(reader.readObject());

        assertEquals(bsonObject, parsed);
    }

    @Test
    public void testEncodeSize() {
        final BsonObject bsonObject = createBsonObject();
        bsonObject.put("foo", "bar");
        Buffer encoded = Buffer.buffer(BsonCodec.bsonObjectToBsonBytes(bsonObject));
        int length = encoded.getIntLE(0);
        assertEquals(encoded.length(), length);
    }

    @Test
    public void testInvalidJson() {
        byte[] invalid = TestUtils.randomByteArray(100);
        try {
            BsonCodec.bsonBytesToBsonObject(invalid);
            fail();
        } catch (Exception e) {
            // OK
        }
    }


    @Test
    public void testEncodeArray() {
        BsonArray bsonArray = new BsonArray();
        bsonArray.add("foo");
        bsonArray.add(123);
        bsonArray.add(1234L);
        bsonArray.add(1.23f);
        bsonArray.add(2.34d);
        bsonArray.add(true);
        byte[] bytes = TestUtils.randomByteArray(10);
        bsonArray.add(bytes);
        bsonArray.addNull();
        bsonArray.add(new BsonObject().put("foo", "bar"));
        bsonArray.add(new BsonArray().add("foo").add(123));

        final byte[] encoded = BsonCodec.bsonArrayToBsonBytes(bsonArray);

        BsonArray arr = BsonCodec.bsonBytesToBsonArray(encoded);
        assertEquals("foo", arr.getString(0));
        assertEquals(Integer.valueOf(123), arr.getInteger(1));
        assertEquals(Long.valueOf(1234l), arr.getLong(2));
        assertEquals(Float.valueOf(1.23f), arr.getFloat(3));
        assertEquals(2.34d, arr.getDouble(4), 0.001);
        assertEquals(true, arr.getBoolean(5));
        assertTrue(TestUtils.byteArraysEqual(bytes, arr.getBinary(6)));
        assertTrue(arr.hasNull(7));
        BsonObject obj = arr.getBsonObject(8);
        assertEquals("bar", obj.getString("foo"));
        BsonArray arr2 = arr.getBsonArray(9);
        assertEquals("foo", arr2.getString(0));
        assertEquals(Integer.valueOf(123), arr2.getInteger(1));
    }


    @Test
    public void testJsonArrayConversion() {
        javax.json.JsonObject jsonObject =
                javax.json.Json.createObjectBuilder().add("foo", "bar").build();
        final BsonObject bsonObject = new BsonObject().put("foo", "bar");
        javax.json.JsonArray jsonArray =
                javax.json.Json.createArrayBuilder().add("foo").add(123).add(jsonObject).build();

        BsonArray bsonArray = BsonCodec.jsonArrayToBsonArray(jsonArray);
        assertEquals(bsonObject, bsonArray.getBsonObject(2));
        assertEquals(123L, bsonArray.getInteger(1).longValue());
        assertEquals("foo", bsonArray.getString(0));

        javax.json.JsonArray jsonArray2 = BsonCodec.bsonArrayToJsonArray(bsonArray);
        assertEquals(jsonArray, jsonArray2);
    }

    @Test
    public void testInvalidArrayJson() {
        byte[] invalid = TestUtils.randomByteArray(100);
        try {
            BsonCodec.bsonBytesToJsonArray(invalid);
            fail();
        } catch (Exception e) {
            // OK
        }
    }

    private BsonObject createBsonObject() {
        BsonObject obj = new BsonObject();
        obj.put("mystr", "bar");
        obj.put("myint", Integer.MAX_VALUE);
        obj.put("mylong", Long.MAX_VALUE);
        obj.put("mydouble", Double.MAX_VALUE);
        obj.put("myboolean", true);
        obj.put("mybinary", TestUtils.randomByteArray(100));
        obj.put("myinstant", Instant.now());
        return obj;
    }


}