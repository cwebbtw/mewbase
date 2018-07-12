package io.mewbase.bson;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;

public class VertxJsonObjectValueVisitor {

    private static final BsonValue.Visitor<Object> objectValueBuilder = new BsonValue.Visitor<Object>() {
        @Override
        public Object visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public Object visit(BsonValue.StringBsonValue value) {
            return value.getValue();
        }

        @Override
        public Object visit(BsonValue.IntBsonValue value) {
            return value.getValue();
        }

        @Override
        public Object visit(BsonValue.LongBsonValue value) {
            return value.getValue();
        }

        @Override
        public Object visit(BsonValue.DoubleBsonValue value) {
            return value.getValue();
        }

        @Override
        public Object visit(BsonValue.FloatBsonValue value) {
            return value.getValue();
        }

        @Override
        public Object visit(BsonValue.BooleanBsonValue value) {
            return value.getValue();
        }

        @Override
        public Object visit(BsonValue.BsonObjectBsonValue value) {
            final Map<String, Object> result = Maps.newHashMap();

            value.getValue().iterator().forEachRemaining(entry -> {
                final Object valueObject = entry.getValue().visit(objectValueBuilder);
                result.put(entry.getKey(), valueObject);
            });

            return result;
        }

        @Override
        public Object visit(BsonValue.BsonArrayBsonValue value) {
            final List<Object> result = Lists.newArrayList();
            value.getValue().iterator().forEachRemaining(bsonValue -> result.add(bsonValue.visit(objectValueBuilder)));
            return result;
        }
    };

    public static JsonObject build(BsonObject object) {
        return new JsonObject((Map<String, Object>) BsonValue.of(object).visit(objectValueBuilder));
    }


}
