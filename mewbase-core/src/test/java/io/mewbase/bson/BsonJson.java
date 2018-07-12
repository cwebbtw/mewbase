package io.mewbase.bson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonModule;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BsonJson {

    @Test
    public void canGenerateBson() throws IOException {
        final JsonObject jsonObject = Json.createObjectBuilder().add("hello", "world").build();
        final ObjectMapper objectMapper = new ObjectMapper(new  BsonFactory());

        objectMapper.registerModules(new BsonModule(), new JSR353Module());

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();


        objectMapper.writeValue(byteArrayOutputStream, jsonObject);

        byte[] result = byteArrayOutputStream.toByteArray();
        final JsonObject rebuilt = objectMapper.readValue(result, JsonObject.class);

        System.out.println(new String(byteArrayOutputStream.toByteArray(), "UTF-8"));
        System.out.println(rebuilt);
    }
}
