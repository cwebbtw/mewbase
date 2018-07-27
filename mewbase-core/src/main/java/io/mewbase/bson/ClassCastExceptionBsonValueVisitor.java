package io.mewbase.bson;

abstract class ClassCastExceptionBsonValueVisitor<T> implements BsonValue.Visitor<T> {
    /*
    These visitors throw a ClassCastException when applied to a BsonValue type they
    are not expecting
     */
    static final BsonValue.Visitor<String> stringVisitor = new ClassCastExceptionBsonValueVisitor<String>() {
        @Override
        public String visit(BsonValue.StringBsonValue value) {
            return value.getValue();
        }
    };

    static final BsonValue.Visitor<Integer> integerVisitor = new ClassCastExceptionBsonValueVisitor<Integer>() {
        @Override
        public Integer visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().intValue();
        }
    };

    static final BsonValue.Visitor<Long> longVisitor = new ClassCastExceptionBsonValueVisitor<Long>() {
        @Override
        public Long visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().longValue();
        }
    };

    static final BsonValue.Visitor<Double> doubleVisitor = new ClassCastExceptionBsonValueVisitor<Double>() {
        @Override
        public Double visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().doubleValue();
        }
    };

    static final BsonValue.Visitor<Float> floatVisitor = new ClassCastExceptionBsonValueVisitor<Float>() {
        @Override
        public Float visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().floatValue();
        }
    };

    static final BsonValue.Visitor<Boolean> booleanVisitor = new ClassCastExceptionBsonValueVisitor<Boolean>() {
        @Override
        public Boolean visit(BsonValue.BooleanBsonValue value) {
            return value.getValue();
        }
    };

    static final BsonValue.Visitor<BsonObject> bsonObjectVisitor = new ClassCastExceptionBsonValueVisitor<BsonObject>() {
        @Override
        public BsonObject visit(BsonValue.BsonObjectBsonValue value) {
            return value.getValue();
        }
    };

    static final BsonValue.Visitor<BsonArray> bsonArrayVisitor = new ClassCastExceptionBsonValueVisitor<BsonArray>() {
        @Override
        public BsonArray visit(BsonValue.BsonArrayBsonValue value) {
            return value.getValue();
        }
    };

    @Override
    public T visit(BsonValue.NullBsonValue nullValue) {
        return null;
    }

    @Override
    public T visit(BsonValue.StringBsonValue value) {
        throw unexpected(value);
    }

    @Override
    public T visit(BsonValue.BooleanBsonValue value) {
        throw unexpected(value);
    }

    @Override
    public T visit(BsonValue.BsonObjectBsonValue value) {
        throw unexpected(value);
    }

    @Override
    public T visit(BsonValue.BsonArrayBsonValue value) {
        throw unexpected(value);
    }

    @Override
    public T visit(BsonValue.BigDecimalBsonValue value) {
        throw unexpected(value);
    }

    private ClassCastException unexpected(BsonValue bsonValue) {
        return new ClassCastException("Unexpected " + bsonValue.getClass().getSimpleName());
    }
}
