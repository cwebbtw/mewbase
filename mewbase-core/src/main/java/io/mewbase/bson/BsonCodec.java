package io.mewbase.bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonModule;

import javax.json.*;
import java.io.IOException;
import java.io.StringReader;
import java.util.function.Consumer;

public class BsonCodec {

    private static final ObjectMapper objectMapper = new ObjectMapper(new BsonFactory());

    static {
        objectMapper.registerModules(new BsonModule(), new JSR353Module());
    }

    public static BsonObject jsonStringToBsonObject(String jsonString) {
        final StringReader stringReader = new StringReader(jsonString);
        final JsonObject jsonObject = Json.createReader(stringReader).readObject();
        return jsonObjectToBsonObject(jsonObject);
    }

    public static BsonArray jsonStringToBsonArray(String jsonString) {
        final StringReader stringReader = new StringReader(jsonString);
        final JsonArray jsonArray = Json.createReader(stringReader).readArray();
        return jsonArrayToBsonArray(jsonArray);
    }

    public static byte[] jsonObjectToBsonBytes(JsonObject jsonObject) {
        try {
            return objectMapper.writeValueAsBytes(jsonObject);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] bsonArrayToBsonBytes(BsonArray bsonArray) {
        return jsonArrayToBsonBytes(bsonArrayToJsonArray(bsonArray));
    }

    public static byte[] jsonArrayToBsonBytes(JsonArray jsonArray) {
        try {
            return objectMapper.writer().forType(JsonArray.class).writeValueAsBytes(jsonArray);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] bsonObjectToBsonBytes(BsonObject bsonObject) {
        return jsonObjectToBsonBytes(bsonObjectToJsonObject(bsonObject));
    }

    public static BsonObject bsonBytesToBsonObject(byte[] bytes) {
        return jsonObjectToBsonObject(bsonBytesToJsonObject(bytes));
    }

    public static BsonArray bsonBytesToBsonArray(byte[] bytes) {
        return jsonArrayToBsonArray(bsonBytesToJsonArray(bytes));
    }

    public static JsonObject bsonBytesToJsonObject(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, JsonObject.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonArray bsonBytesToJsonArray(byte[] bytes) {
        try {
            final JsonObject jsonObject = objectMapper.readerFor(JsonObject.class).readValue(bytes);
            final JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
            jsonObject.values().forEach(jsonArrayBuilder::add);
            return jsonArrayBuilder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonObject bsonObjectToJsonObject(BsonObject bsonObject) {
        final JsonObjectBuilder builder = Json.createObjectBuilder();
        final ObjectBuilderVisitor visitor = new ObjectBuilderVisitor(builder);

        /*
        There's a bit of indirection going on here, as you can't construct JsonValue instances.
        ObjectBuilderVisitor's visit returns a function FieldName -> Void, which when called will
        register the JsonValue as a field with the provided name.
         */
        bsonObject.iterator().forEachRemaining(entry -> entry.getValue().visit(visitor).accept(entry.getKey()));

        return builder.build();
    }

    public static JsonArray bsonArrayToJsonArray(BsonArray bsonArray) {
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        final ArrayBuilderVistior visitor = new ArrayBuilderVistior(builder);

        bsonArray.iterator().forEachRemaining(element -> element.visit(visitor));

        return builder.build();
    }

    public static BsonObject jsonObjectToBsonObject(JsonObject jsonObject) {
        final BsonObject result = new BsonObject();
        jsonObject.forEach((field, value) -> {
            result.put(field, toBsonValue(value));
        });
        return result;
    }

    public static BsonArray jsonArrayToBsonArray(JsonArray jsonArray) {
        final BsonArray result = new BsonArray();
        jsonArray.forEach(element -> {
            result.add(toBsonValue(element));
        });
        return result;
    }

    private static class ArrayBuilderVistior implements BsonValue.Visitor<Void> {

        private final JsonArrayBuilder builder;

        private ArrayBuilderVistior(JsonArrayBuilder builder) {
            this.builder = builder;
        }

        @Override
        public Void visit(BsonValue.NullBsonValue nullValue) {
            builder.addNull();
            return null;
        }

        @Override
        public Void visit(BsonValue.StringBsonValue value) {
            builder.add(value.getValue());
            return null;
        }

        @Override
        public Void visit(BsonValue.BigDecimalBsonValue value) {
            builder.add(value.getValue());
            return null;
        }

        @Override
        public Void visit(BsonValue.BooleanBsonValue value) {
            builder.add(value.getValue());
            return null;
        }

        @Override
        public Void visit(BsonValue.BsonObjectBsonValue value) {
            builder.add(bsonObjectToJsonObject(value.getValue()));
            return null;
        }

        @Override
        public Void visit(BsonValue.BsonArrayBsonValue value) {
            builder.add(bsonArrayToJsonArray(value.getValue()));
            return null;
        }
    }

    private static class ObjectBuilderVisitor implements BsonValue.Visitor<Consumer<String>> {

        private final JsonObjectBuilder builder;

        private ObjectBuilderVisitor(JsonObjectBuilder builder) {
            this.builder = builder;
        }

        @Override
        public Consumer<String> visit(BsonValue.NullBsonValue nullValue) {
            return builder::addNull;
        }

        @Override
        public Consumer<String> visit(BsonValue.StringBsonValue value) {
            return fieldName -> builder.add(fieldName, value.getValue());
        }

        @Override
        public Consumer<String> visit(BsonValue.BigDecimalBsonValue value) {
            return fieldName -> builder.add(fieldName, value.getValue());
        }

        @Override
        public Consumer<String> visit(BsonValue.BooleanBsonValue value) {
            return fieldName -> builder.add(fieldName, value.getValue());
        }

        @Override
        public Consumer<String> visit(BsonValue.BsonObjectBsonValue value) {
            return fieldName -> {
                final JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder();
                final ObjectBuilderVisitor innerVisitor = new ObjectBuilderVisitor(jsonObjectBuilder);

                value.getValue().iterator().forEachRemaining(entry -> {
                    entry.getValue().visit(innerVisitor).accept(entry.getKey());
                });

                builder.add(fieldName, jsonObjectBuilder.build());
            };
        }

        @Override
        public Consumer<String> visit(BsonValue.BsonArrayBsonValue value) {
            return fieldName -> builder.add(fieldName, bsonArrayToJsonArray(value.getValue()));
        }
    }

    private static BsonValue toBsonValue(JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
            case STRING:
                return BsonValue.of(((JsonString) jsonValue).getString());
            case OBJECT:
                final JsonObject jsonObject = (JsonObject) jsonValue;
                final BsonObject bsonObject = new BsonObject();

                jsonObject.forEach((fieldName, value) -> bsonObject.put(fieldName, toBsonValue(value)));

                return BsonValue.of(bsonObject);
            case ARRAY:
                final JsonArray jsonArray = (JsonArray) jsonValue;
                final BsonArray bsonArray = new BsonArray();

                jsonArray.forEach(value -> bsonArray.add(toBsonValue(value)));

                return BsonValue.of(bsonArray);
            case NUMBER:
                final JsonNumber jsonNumber = (JsonNumber) jsonValue;
                if (jsonNumber.isIntegral())
                    return BsonValue.of(jsonNumber.longValue());
                else
                    return BsonValue.of(jsonNumber.doubleValue());
            case TRUE:
                return BsonValue.of(true);
            case FALSE:
                return BsonValue.of(false);
            case NULL:
                return BsonValue.nullValue();
            default:
                throw new IllegalArgumentException("Invalid JsonValue type " + jsonValue.getValueType().name());
        }
    }
}
