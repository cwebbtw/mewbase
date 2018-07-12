package io.mewbase.bson;

import org.junit.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import static org.junit.Assert.*;

public class BsonToJavaxJsonTest {

    @Test
    public void convertEmptyObject() {
        assertEquals(Json.createObjectBuilder().build(), BsonToJavaxJson.convert(new BsonObject()));
    }

    @Test
    public void convertObjectWithNullField() {
        assertEquals(Json.createObjectBuilder().addNull("woah").build(), BsonToJavaxJson.convert(new BsonObject().putNull("woah")));
    }

    @Test
    public void convertObjectWithStringField() {
        assertEquals(Json.createObjectBuilder().add("hello", "world").build(),
                BsonToJavaxJson.convert(new BsonObject().put("hello", "world")));
    }

    @Test
    public void convertObjectWithIntField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123).build(),
                BsonToJavaxJson.convert(new BsonObject().put("hello", 123)));
    }

    @Test
    public void convertObjectWithLongField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123).build(),
                BsonToJavaxJson.convert(new BsonObject().put("hello", 123L)));
    }

    @Test
    public void convertObjectWithDoubleField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123d).build(),
                BsonToJavaxJson.convert(new BsonObject().put("hello", 123.0d)));
    }

    @Test
    public void convertObjectWithFloatField() {
        assertEquals(Json.createObjectBuilder().add("hello", 123f).build(),
                BsonToJavaxJson.convert(new BsonObject().put("hello", 123.0f)));
    }

    @Test
    public void convertObjectWithBooleanField() {
        assertEquals(Json.createObjectBuilder().add("hello", true).build(),
                BsonToJavaxJson.convert(new BsonObject().put("hello", true)));
    }

    @Test
    public void convertObjectWithObjectField() {
        final BsonObject innerBson = new BsonObject().put("mind", "blown");
        final BsonObject outerBson = new BsonObject().put("inception", innerBson);

        final JsonObjectBuilder innerJson = Json.createObjectBuilder().add("mind", "blown");
        final JsonObject outerJson = Json.createObjectBuilder().add("inception", innerJson).build();

        assertEquals(outerJson,
                BsonToJavaxJson.convert(outerBson));
    }

    @Test
    public void convertObjectWithArrayField() {
        final BsonObject bson = new BsonObject().put("mind", new BsonArray().add("blown"));
        final JsonObject json = Json.createObjectBuilder().add("mind", Json.createArrayBuilder().add("blown").build()).build();

        assertEquals(json,
                BsonToJavaxJson.convert(bson));
    }

    @Test
    public void convertEmptyArray() {
        assertEquals(Json.createArrayBuilder().build(), BsonToJavaxJson.convert(new BsonArray()));
    }

    @Test
    public void convertArrayWithNullElement() {
        assertEquals(Json.createArrayBuilder().addNull().build(), BsonToJavaxJson.convert(new BsonArray().addNull()));
    }

    @Test
    public void convertArrayWithStringElement() {
        assertEquals(Json.createArrayBuilder().add("hello").build(),
                BsonToJavaxJson.convert(new BsonArray().add("hello")));
    }

    @Test
    public void convertArrayWithIntElement() {
        assertEquals(Json.createArrayBuilder().add(123).build(),
                BsonToJavaxJson.convert(new BsonArray().add(123)));
    }

    @Test
    public void convertArrayWithLongElement() {
        assertEquals(Json.createArrayBuilder().add(123L).build(),
                BsonToJavaxJson.convert(new BsonArray().add(123L)));
    }

    @Test
    public void convertArrayWithDoubleElement() {
        assertEquals(Json.createArrayBuilder().add(123d).build(),
                BsonToJavaxJson.convert(new BsonArray().add(123d)));
    }

    @Test
    public void convertArrayWithFloatElement() {
        assertEquals(Json.createArrayBuilder().add(123f).build(),
                BsonToJavaxJson.convert(new BsonArray().add(123f)));
    }

    @Test
    public void convertArrayWithBooleanElement() {
        assertEquals(Json.createArrayBuilder().add(true).build(),
                BsonToJavaxJson.convert(new BsonArray().add(true)));
    }

    @Test
    public void convertArrayWithObjectElement() {
        final BsonObject bsonObject = new BsonObject().put("mind", "blown");
        final JsonObject jsonObject = Json.createObjectBuilder().add("mind", "blown").build();

        assertEquals(Json.createArrayBuilder().add(jsonObject).build(),
                BsonToJavaxJson.convert(new BsonArray().add(bsonObject)));
    }

    @Test
    public void convertArrayWithArrayElement() {
        final BsonArray innerBson = new BsonArray().add("mind").add("blown");
        final JsonArray innerJson = Json.createArrayBuilder().add("mind").add("blown").build();

        assertEquals(Json.createArrayBuilder().add(innerJson).build(),
                BsonToJavaxJson.convert(new BsonArray().add(innerBson)));
    }


}