package io.mewbase.bson;

import javax.json.*;
import java.util.function.Consumer;

public class BsonToJavaxJson {

    public static JsonObject convert(BsonObject bsonObject) {
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

    public static JsonArray convert(BsonArray bsonArray) {
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        final ArrayBuilderVistior visitor = new ArrayBuilderVistior(builder);

        bsonArray.iterator().forEachRemaining(element -> element.visit(visitor));

        return builder.build();
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
        public Void visit(BsonValue.IntBsonValue value) {
            builder.add(value.getValue());
            return null;
        }

        @Override
        public Void visit(BsonValue.LongBsonValue value) {
            builder.add(value.getValue());
            return null;
        }

        @Override
        public Void visit(BsonValue.DoubleBsonValue value) {
            builder.add(value.getValue());
            return null;
        }

        @Override
        public Void visit(BsonValue.FloatBsonValue value) {
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
            builder.add(convert(value.getValue()));
            return null;
        }

        @Override
        public Void visit(BsonValue.BsonArrayBsonValue value) {
            builder.add(convert(value.getValue()));
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
        public Consumer<String> visit(BsonValue.IntBsonValue value) {
            return fieldName -> builder.add(fieldName, value.getValue());
        }

        @Override
        public Consumer<String> visit(BsonValue.LongBsonValue value) {
            return fieldName -> builder.add(fieldName, value.getValue());
        }

        @Override
        public Consumer<String> visit(BsonValue.DoubleBsonValue value) {
            return fieldName -> builder.add(fieldName, value.getValue());
        }

        @Override
        public Consumer<String> visit(BsonValue.FloatBsonValue value) {
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
            return fieldName -> builder.add(fieldName, convert(value.getValue()));
        }
    }
}
