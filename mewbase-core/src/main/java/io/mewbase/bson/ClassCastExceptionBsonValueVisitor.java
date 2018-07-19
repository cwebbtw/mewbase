package io.mewbase.bson;

abstract class ClassCastExceptionBsonValueVisitor<T> implements BsonValue.Visitor<T> {
    static final BsonValue.Visitor<String> stringVisitor = new ClassCastExceptionBsonValueVisitor<String>() {
        @Override
        public String visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public String visit(BsonValue.StringBsonValue value) {
            return value.getValue();
        }
    };
    static final BsonValue.Visitor<Integer> integerVisitor = new ClassCastExceptionBsonValueVisitor<Integer>() {

        @Override
        public Integer visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public Integer visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().intValue();
        }

    };
    static final BsonValue.Visitor<Long> longVisitor = new ClassCastExceptionBsonValueVisitor<Long>() {

        @Override
        public Long visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public Long visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().longValue();
        }

    };
    static final BsonValue.Visitor<Double> doubleVisitor = new ClassCastExceptionBsonValueVisitor<Double>() {

        @Override
        public Double visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public Double visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().doubleValue();
        }

    };
    static final BsonValue.Visitor<Float> floatVisitor = new ClassCastExceptionBsonValueVisitor<Float>() {
        @Override
        public Float visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }


        @Override
        public Float visit(BsonValue.BigDecimalBsonValue value) {
            return value.getValue().floatValue();
        }

    };
    static final BsonValue.Visitor<Boolean> booleanVisitor = new ClassCastExceptionBsonValueVisitor<Boolean>() {
        @Override
        public Boolean visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public Boolean visit(BsonValue.BooleanBsonValue value) {
            return value.getValue();
        }
    };
    static final BsonValue.Visitor<BsonObject> bsonObjectVisitor = new ClassCastExceptionBsonValueVisitor<BsonObject>() {
        @Override
        public BsonObject visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public BsonObject visit(BsonValue.BsonObjectBsonValue value) {
            return value.getValue();
        }
    };
    static final BsonValue.Visitor<BsonArray> bsonArrayVisitor = new ClassCastExceptionBsonValueVisitor<BsonArray>() {
        @Override
        public BsonArray visit(BsonValue.NullBsonValue nullValue) {
            return null;
        }

        @Override
        public BsonArray visit(BsonValue.BsonArrayBsonValue value) {
            return value.getValue();
        }
    };

    @Override
    public T visit(BsonValue.NullBsonValue nullValue) {
        throw new ClassCastException("Unexpected " + nullValue.getClass().getSimpleName());
    }

    @Override
    public T visit(BsonValue.StringBsonValue value) {
        throw new ClassCastException("Unexpected " + value.getClass().getSimpleName());
    }

    @Override
    public T visit(BsonValue.BooleanBsonValue value) {
        throw new ClassCastException("Unexpected " + value.getClass().getSimpleName());
    }

    @Override
    public T visit(BsonValue.BsonObjectBsonValue value) {
        throw new ClassCastException("Unexpected " + value.getClass().getSimpleName());
    }

    @Override
    public T visit(BsonValue.BsonArrayBsonValue value) {
        throw new ClassCastException("Unexpected " + value.getClass().getSimpleName());
    }

    @Override
    public T visit(BsonValue.BigDecimalBsonValue value) {
        throw new ClassCastException("Unexpected " + value.getClass().getSimpleName());
    }
}
