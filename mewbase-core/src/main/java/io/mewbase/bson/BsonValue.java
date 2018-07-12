package io.mewbase.bson;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public abstract class BsonValue {

    private BsonValue() {
    }

    public static BsonValue fromObjectUnsafe(Object object) {
        if (object == null) {
            return NullBsonValue.INSTANCE;
        }
        else if (object instanceof String) {
            return of((String) object);
        }
        else if (object instanceof Integer) {
            return of((Integer) object);
        }
        else if (object instanceof java.lang.Long) {
            return of((java.lang.Long) object);
        }
        else if (object instanceof Double) {
            return of((Double) object);
        }
        else if (object instanceof Float) {
            return of((Float) object);
        }
        else if (object instanceof Boolean) {
            return of((Boolean) object);
        }
        else if (object instanceof Map<?, ?>) {
            final Map<String, Object> objectMap = (Map<String, Object>) object;
            final Map<String, BsonValue> bsonValueMap = Maps.transformValues(objectMap, BsonValue::fromObjectUnsafe);
            return of(bsonValueMap);
        }
        else if (object instanceof List<?>) {
            final List<Object> objectList= (List<Object>) object;
            final List<BsonValue> bsonValueList = Lists.transform(objectList, BsonValue::fromObjectUnsafe);
            return of(bsonValueList);
        }
        else {
            throw new IllegalArgumentException("Could not convert value of a " + object.getClass().getSimpleName() + " to a BsonValue");
        }
    }

    public interface Visitor<Res> {
        Res visit(NullBsonValue nullValue);
        Res visit(StringBsonValue value);
        Res visit(IntBsonValue value);
        Res visit(LongBsonValue value);
        Res visit(DoubleBsonValue value);
        Res visit(FloatBsonValue value);
        Res visit(BooleanBsonValue value);
        Res visit(BsonObjectBsonValue value);
        Res visit(BsonArrayBsonValue value);
    }

    public abstract <Res> Res visit(Visitor<Res> res);
    public abstract BsonValue copy();
    public abstract boolean isNull();

    private static <T> BsonValue fromNullable(T value, Function<T, BsonValue> builder) {
        if (value == null)
            return NullBsonValue.INSTANCE;
        else
            return builder.apply(value);
    }

    public static BsonValue nullValue() {
        return NullBsonValue.INSTANCE;
    }

    public static BsonValue of(CharSequence value) {
        final String string = value == null ? null : value.toString();
        return fromNullable(string, StringBsonValue::new);
    }

    public static IntBsonValue of(int value) {
        return new IntBsonValue(value);
    }

    public static BsonValue of(Integer value) {
        return fromNullable(value, IntBsonValue::new);
    }

    public static LongBsonValue of(long value) {
        return new LongBsonValue(value);
    }

    public static BsonValue of(java.lang.Long value) {
        return fromNullable(value, LongBsonValue::new);
    }

    public static DoubleBsonValue of(double value) {
        return new DoubleBsonValue(value);
    }

    public static BsonValue of(Double value) {
        return fromNullable(value, DoubleBsonValue::new);
    }

    public static FloatBsonValue of(float value) {
        return new FloatBsonValue(value);
    }

    public static BsonValue of(Float value) {
        return fromNullable(value, FloatBsonValue::new);
    }

    public static BooleanBsonValue of(boolean value) {
        return new BooleanBsonValue(value);
    }

    public static BsonValue of(Boolean value) {
        return fromNullable(value, BooleanBsonValue::new);
    }

    public static BsonValue of(BsonObject value) {
        return fromNullable(value, BsonObjectBsonValue::new);
    }

    public static BsonValue of(Map<String, BsonValue> value) {
        return of(new BsonObject(value));
    }

    public static BsonValue of(BsonArray value) {
        return fromNullable(value, BsonArrayBsonValue::new);
    }

    public static BsonValue of(List<BsonValue> value) {
        return of(new BsonArray(value));
    }

    public static final class StringBsonValue extends BsonValue {
        private final String value;

        private StringBsonValue(String value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return this; // immutable
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringBsonValue that = (StringBsonValue) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "StringBsonValue{" +
                    "value='" + value + '\'' +
                    '}';
        }
    }

    public static final class IntBsonValue extends BsonValue {
        private final int value;

        private IntBsonValue(int value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return this; // immutable
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public int getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IntBsonValue that = (IntBsonValue) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "IntBsonValue{" +
                    "value=" + value +
                    '}';
        }
    }

    public static final class LongBsonValue extends BsonValue {
        private final long value;

        private LongBsonValue(long value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return this; // immutable
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public long getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LongBsonValue that = (LongBsonValue) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "LongBsonValue{" +
                    "value=" + value +
                    '}';
        }
    }

    public static final class DoubleBsonValue extends BsonValue {
        private final double value;

        private DoubleBsonValue(double value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return this; // immutable
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public double getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DoubleBsonValue that = (DoubleBsonValue) o;
            return Double.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "DoubleBsonValue{" +
                    "value=" + value +
                    '}';
        }
    }

    public static final class FloatBsonValue extends BsonValue {
        private final float value;

        private FloatBsonValue(float value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return this; // immutable
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public float getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FloatBsonValue that = (FloatBsonValue) o;
            return Float.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "FloatBsonValue{" +
                    "value=" + value +
                    '}';
        }
    }

    public static final class BooleanBsonValue extends BsonValue {
        private final boolean value;

        private BooleanBsonValue(boolean value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return this; // immutable
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BooleanBsonValue that = (BooleanBsonValue) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "BooleanBsonValue{" +
                    "value=" + value +
                    '}';
        }
    }

    public static final class NullBsonValue extends BsonValue {
        private static final NullBsonValue INSTANCE = new NullBsonValue();

        private NullBsonValue() {
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public BsonValue copy() {
            return this; // immutable
        }

        @Override
        public String toString() {
            return "NullBsonValue{}";
        }
    }

    public static final class BsonObjectBsonValue extends BsonValue {
        private final BsonObject value;

        private BsonObjectBsonValue(BsonObject value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return of(value.copy());
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public BsonObject getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BsonObjectBsonValue that = (BsonObjectBsonValue) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "BsonObjectBsonValue{" +
                    "value=" + value +
                    '}';
        }
    }

    public static final class BsonArrayBsonValue extends BsonValue {
        private final BsonArray value;

        private BsonArrayBsonValue(BsonArray value) {
            this.value = value;
        }

        @Override
        public <Res> Res visit(Visitor<Res> res) {
            return res.visit(this);
        }

        @Override
        public BsonValue copy() {
            return of(value.copy());
        }

        @Override
        public boolean isNull() {
            return false;
        }

        public BsonArray getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BsonArrayBsonValue that = (BsonArrayBsonValue) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "BsonArrayBsonValue{" +
                    "value=" + value +
                    '}';
        }
    }

}
