package io.mewbase.bson;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public abstract class BsonValue {

    private BsonValue() {
    }


    public interface Visitor<Res> {
        Res visit(NullBsonValue nullValue);
        Res visit(StringBsonValue value);
        Res visit(BigDecimalBsonValue value);
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

    public static BigDecimalBsonValue of(long value) {
        return new BigDecimalBsonValue(new BigDecimal(value));
    }

    public static BsonValue of(java.lang.Integer value) {
        return fromNullable(value, i -> of(i.longValue()));
    }

    public static BsonValue of(java.lang.Long value) {
        return fromNullable(value, l -> of(l.longValue()));
    }

    public static BsonValue of(java.lang.Float value) {
        return fromNullable(value, f -> of(f.doubleValue()));
    }

    public static BigDecimalBsonValue of(double value) {
        return new BigDecimalBsonValue(new BigDecimal(value));
    }

    public static BsonValue of(Double value) {
        return fromNullable(value, d -> of(d.doubleValue()));
    }

    public static BooleanBsonValue of(boolean value) {
        return BooleanBsonValue.from(value);
    }

    public static BsonValue of(Boolean value) {
        return fromNullable(value, BooleanBsonValue::from);
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

    public static final class BigDecimalBsonValue extends BsonValue {

        private final BigDecimal value;

        private BigDecimalBsonValue(BigDecimal value) {
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

        public BigDecimal getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BigDecimalBsonValue that = (BigDecimalBsonValue) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "BigDecimalBsonValue{" +
                    "value=" + value +
                    '}';
        }
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

    public static final class BooleanBsonValue extends BsonValue {
        private final boolean value;

        public static final BooleanBsonValue TRUE = new BooleanBsonValue(true);
        public static final BooleanBsonValue FALSE = new BooleanBsonValue(false);

        public static BooleanBsonValue from(boolean value) {
            return value ? TRUE : FALSE;
        }

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
