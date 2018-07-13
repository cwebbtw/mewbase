package io.mewbase.bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonModule;

import javax.json.*;
import java.io.IOException;

public class BsonCodec {

    private static final ObjectMapper objectMapper = new ObjectMapper(new BsonFactory());

    static {
        objectMapper.registerModules(new BsonModule(), new JSR353Module());
    }

    public static byte[] fromJsonObject(JsonObject jsonObject) {
        try {
            return objectMapper.writeValueAsBytes(jsonObject);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] fromJsonArray(JsonArray jsonArray) {
        try {
            return objectMapper.writer().forType(JsonArray.class).writeValueAsBytes(jsonArray);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonObject toJsonObject(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, JsonObject.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonArray toJsonArray(byte[] bytes) {
        try {
            final JsonObject jsonObject = objectMapper.readerFor(JsonObject.class).readValue(bytes);
            final JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
            jsonObject.values().forEach(jsonArrayBuilder::add);
            return jsonArrayBuilder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
