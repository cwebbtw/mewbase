package io.mewbase.bson;

import scala.Long;

import java.util.function.Function;

public abstract class BsonValue {

    private BsonValue() {
    }

    public static BsonValue fromObjectUnsafe(Object object) {
        if (object == null) {
            return NullBsonValue.INSTANCE;
        }
        else if (object instanceof String) {
            return from((String) object);
        }
        else if (object instanceof Integer) {
            return from((Integer) object);
        }
        else if (object instanceof java.lang.Long) {
            return from((java.lang.Long) object);
        }
        else if (object instanceof Double) {
            return from((Double) object);
        }
        else if (object instanceof Float) {
            return from((Float) object);
        }
        else if (object instanceof Boolean) {
            return from((Boolean) object);
        }
        else if (object instanceof BsonObject) {
            return from((BsonObject) object);
        }
        else if (object instanceof BsonArray) {
            return from((BsonArray) object);
        }
        else {
            throw new IllegalArgumentException("Could not convert value from a " + object.getClass().getSimpleName() + " to a BsonValue");
        }
    }

    private static <T> BsonValue fromNullable(T value, Function<T, BsonValue> builder) {
        if (value == null)
            return NullBsonValue.INSTANCE;
        else
            return builder.apply(value);
    }

    public static BsonValue from(String value) {
        return fromNullable(value, StringBsonValue::new);
    }

    public static IntBsonValue from(int value) {
        return new IntBsonValue(value);
    }

    public static BsonValue from(Integer value) {
        return fromNullable(value, IntBsonValue::new);
    }

    public static LongBsonValue from(long value) {
        return new LongBsonValue(value);
    }

    public static BsonValue from(java.lang.Long value) {
        return fromNullable(value, LongBsonValue::new);
    }

    public static DoubleBsonValue from(double value) {
        return new DoubleBsonValue(value);
    }

    public static BsonValue from(Double value) {
        return fromNullable(value, DoubleBsonValue::new);
    }

    public static FloatBsonValue from(float value) {
        return new FloatBsonValue(value);
    }

    public static BsonValue from(Float value) {
        return fromNullable(value, FloatBsonValue::new);
    }

    public static BooleanBsonValue from(boolean value) {
        return new BooleanBsonValue(value);
    }

    public static BsonValue from(Boolean value) {
        return fromNullable(value, BooleanBsonValue::new);
    }

    public static BsonValue from(BsonObject value) {
        return fromNullable(value, BsonObjectBsonValue::new);
    }

    public static BsonValue from(BsonArray value) {
        return fromNullable(value, BsonArrayBsonValue::new);
    }

    public static final class StringBsonValue extends BsonValue {
        private final String value;

        private StringBsonValue(String value) {
            this.value = value;
        }
    }

    public static final class IntBsonValue extends BsonValue {
        private final int value;

        private IntBsonValue(int value) {
            this.value = value;
        }
    }

    public static final class LongBsonValue extends BsonValue {
        private final long value;

        private LongBsonValue(long value) {
            this.value = value;
        }
    }

    public static final class DoubleBsonValue extends BsonValue {
        private final double value;

        private DoubleBsonValue(double value) {
            this.value = value;
        }
    }

    public static final class FloatBsonValue extends BsonValue {
        private final float value;

        private FloatBsonValue(float value) {
            this.value = value;
        }
    }

    public static final class BooleanBsonValue extends BsonValue {
        private final boolean value;

        private BooleanBsonValue(boolean value) {
            this.value = value;
        }
    }

    public static final class NullBsonValue extends BsonValue {
        public static final NullBsonValue INSTANCE = new NullBsonValue();

        private NullBsonValue() {
        }
    }

    public static final class BsonObjectBsonValue extends BsonValue {
        private final BsonObject value;

        private BsonObjectBsonValue(BsonObject value) {
            this.value = value;
        }
    }

    public static final class BsonArrayBsonValue extends BsonValue {
        private final BsonArray value;

        private BsonArrayBsonValue(BsonArray value) {
            this.value = value;
        }
    }

}
