package io.mewbase.bson;

import org.junit.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;

public class BsonCodecTest {

    @Test
    public void jsonObjectTest() {
        final JsonObject jsonObject =
                Json.createObjectBuilder().add("hello", "world").build();
        assertEquals(jsonObject, BsonCodec.toJsonObject(BsonCodec.fromJsonObject(jsonObject)));
    }

    @Test
    public void jsonArrayTest() throws UnsupportedEncodingException {
        final JsonArray jsonArray =
                Json.createArrayBuilder().add(123).build();
        byte[] bytes = BsonCodec.fromJsonArray(jsonArray);
        System.out.println(new String(bytes, "UTF-8"));
        assertEquals(jsonArray, BsonCodec.toJsonArray(bytes));
    }

}