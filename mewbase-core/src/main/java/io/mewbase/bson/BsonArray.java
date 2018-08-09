/*
 * Copyright (c) 2011-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Derived from original file JsonArray.java from Vert.x
 */

package io.mewbase.bson;

import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

/**
 * A representation of a <a href="http://json.org/">JSON</a> array in Java.
 * <p>
 * Unlike some other languages Java does not have a native understanding of JSON. To enable JSON to be used easily
 * in Vert.x code we use this class to encapsulate the notion of a JSON array.
 * <p>
 * The implementation adheres to the <a href="http://rfc-editor.org/rfc/rfc7493.txt">RFC-7493</a> to support Temporal
 * data types as well as binary data.
 * <p>
 * Please see the documentation for more information.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BsonArray implements Iterable<BsonValue> {

    private List<BsonValue> list;

    /**
     * Create an empty instance
     */
    public BsonArray() {
        list = new ArrayList<>();
    }

    /**
     * Create an instance from a List. The List is not copied.
     *
     * @param list
     */
    BsonArray(List<BsonValue> list) {
        this.list = list;
    }

    public BsonValue getBsonValue(int pos) {
        return list.get(pos);
    }

    public BsonArray set(int index, BsonValue bsonValue) {
        list.set(index, bsonValue);
        return this;
    }

    /**
     * Get the String at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the String, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to String
     */
    public String getString(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.stringVisitor);
    }

    /**
     * Get the Integer at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Integer, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to Integer
     */
    public Integer getInteger(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.integerVisitor);
    }

    /**
     * Get the Long at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Long, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to Long
     */
    public Long getLong(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.longVisitor);
    }

    /**
     * Get the Double at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Double, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to Double
     */
    public Double getDouble(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.doubleVisitor);
    }

    /**
     * Get the Float at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Float, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to Float
     */
    public Float getFloat(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.floatVisitor);
    }

    /**
     * Get the Boolean at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Boolean, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to Integer
     */
    public Boolean getBoolean(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.booleanVisitor);
    }

    /**
     * Get the BsonObject at position {@code pos} in the array.
     *
     * @param pos the position in the array
     * @return the Integer, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to BsonObject
     */
    public BsonObject getBsonObject(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.bsonObjectVisitor);
    }

    /**
     * Get the BsonArray at position {@code pos} in the array.
     *
     * @param pos the position in the array
     * @return the Integer, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to BsonArray
     */
    public BsonArray getBsonArray(int pos) {
        return getBsonValue(pos).visit(ClassCastExceptionBsonValueVisitor.bsonArrayVisitor);
    }

    /**
     * Get the byte[] at position {@code pos} in the array.
     * <p>
     * JSON itself has no notion of a binary, so this method assumes there is a String value and
     * it contains a Base64 encoded binary, which it decodes if found and returns.
     * <p>
     * This method should be used in conjunction with {@link #add(byte[])}
     *
     * @param pos the position in the array
     * @return the byte[], or null if a null value present
     * @throws ClassCastException if the value cannot be converted to String
     */
    public byte[] getBinary(int pos) {
        final String val = getString(pos);
        if (val == null) {
            return null;
        } else {
            return Base64.getDecoder().decode(val);
        }
    }

    /**
     * Get the Instant at position {@code pos} in the array.
     * <p>
     * JSON itself has no notion of a temporal types, so this method assumes there is a String value and
     * it contains a ISOString encoded date, which it decodes if found and returns.
     * <p>
     * This method should be used in conjunction with {@link #add(Instant)}
     *
     * @param pos the position in the array
     * @return the Instant, or null if a null value present
     * @throws ClassCastException if the value cannot be converted to String
     */
    public Instant getInstant(int pos) {
        final String val = getString(pos);
        if (val == null) {
            return null;
        } else {
            return Instant.from(ISO_INSTANT.parse(val));
        }
    }

    /**
     * Is there a null value at position pos?
     *
     * @param pos the position in the array
     * @return true if null value present, false otherwise
     */
    public boolean hasNull(int pos) {
        return getBsonValue(pos).isNull();
    }

    public BsonArray add(BsonValue value) {
        Objects.requireNonNull(value);
        list.add(value);
        return this;
    }

    /**
     * Add an enum to the JSON array.
     * <p>
     * JSON has no concept of encoding Enums, so the Enum will be converted to a String using the {@link Enum#name}
     * method and the value added as a String.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(Enum value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value.name()));
        return this;
    }

    /**
     * Add a CharSequence to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(CharSequence value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value.toString()));
        return this;
    }

    /**
     * Add a String to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(String value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add an Integer to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(Integer value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add a Long to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(Long value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add a Double to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(Double value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add a Float to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(Float value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add a Boolean to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(Boolean value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add a null value to the JSON array.
     *
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray addNull() {
        list.add(BsonValue.nullValue());
        return this;
    }

    /**
     * Add a JSON object to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(BsonObject value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add another JSON array to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(BsonArray value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(value));
        return this;
    }

    /**
     * Add a binary value to the JSON array.
     * <p>
     * JSON has no notion of binary so the binary will be base64 encoded to a String, and the String added.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(byte[] value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(Base64.getEncoder().encodeToString(value)));
        return this;
    }

    /**
     * Add a Instant value to the JSON array.
     * <p>
     * JSON has no notion of Temporal data so the Instant will be ISOString encoded, and the String added.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray add(Instant value) {
        Objects.requireNonNull(value);
        list.add(BsonValue.of(ISO_INSTANT.format(value)));
        return this;
    }

    /**
     * Appends all of the elements in the specified array to the end of this JSON array.
     *
     * @param array the array
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray addAll(BsonArray array) {
        Objects.requireNonNull(array);
        list.addAll(array.list);
        return this;
    }

    /**
     * Does the JSON array contain the specified value? This method will scan the entire array until it finds a value
     * or reaches the end.
     *
     * @param value the value
     * @return true if it contains the value, false if not
     */
    public boolean contains(BsonValue value) {
        return list.contains(value);
    }

    /**
     * Remove the specified value from the JSON array. This method will scan the entire array until it finds a value
     * or reaches the end.
     *
     * @param value the value to remove
     * @return true if it removed it, false if not found
     */
    public boolean remove(BsonValue value) {
        return list.remove(value);
    }

    /**
     * Remove the value at the specified position in the JSON array.
     *
     * @param pos the position to remove the value at
     * @return the removed value if removed, null otherwise. If the value is a Map, a {@link BsonObject} is built from
     * this Map and returned. It the value is a List, a {@link BsonArray} is built form this List and returned.
     */
    public BsonValue remove(int pos) {
        return list.remove(pos);
    }

    /**
     * Get the number of values in this JSON array
     *
     * @return the number of items
     */
    public int size() {
        return list.size();
    }

    /**
     * Are there zero items in this JSON array?
     *
     * @return true if zero, false otherwise
     */
    public boolean isEmpty() {
        return list.isEmpty();
    }

    /**
     * Get the underlying List
     *
     * @return the underlying List
     */
    List<BsonValue> getList() {
        return list;
    }

    /**
     * Remove all entries from the JSON array
     *
     * @return a reference to this, so the API can be used fluently
     */
    public BsonArray clear() {
        list.clear();
        return this;
    }

    /**
     * Get an Iterator over the values in the JSON array
     *
     * @return an iterator
     */
    @Override
    public Iterator<BsonValue> iterator() {
        return list.iterator();
    }

    public static BsonArray from(Stream<String> stringStream) {
        final BsonArray result = new BsonArray();
        stringStream.forEach(result::add);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("BsonArray{");

        final List<String> elements = new ArrayList<>();
        for (BsonValue value : list) {
            elements.add(value.toString());
        }

        stringBuilder.append(String.join(", ", elements));
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    /**
     * Make a copy of the JSON array
     *
     * @return a copy
     */
    public BsonArray copy() {
        List<BsonValue> copiedList = new ArrayList<>(list.size());
        for (BsonValue val : list) {
            copiedList.add(val.copy());
        }
        return new BsonArray(copiedList);
    }

    /**
     * Get a Stream over the entries in the JSON array
     *
     * @return a Stream
     */
    public Stream<BsonValue> stream() {
        return Bson.asStream(iterator());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BsonArray that = (BsonArray) o;
        return Objects.equals(list, that.list);
    }

    @Override
    public int hashCode() {
        return Objects.hash(list);
    }
}
