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
 *
 * Derived from original file JsonObject.java from Vert.x
 */

package io.mewbase.bson;

import io.mewbase.binders.KeyVal;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

/**
 * A representation of a <a href="http://json.org/">JSON</a> object in Java.
 * <p>
 * Unlike some other languages Java does not have a native understanding of JSON. To enable JSON to be used easily
 * in Vert.x code we use this class to encapsulate the notion of a JSON object.
 * <p>
 * The implementation adheres to the <a href="http://rfc-editor.org/rfc/rfc7493.txt">RFC-7493</a> to support Temporal
 * data types as well as binary data.
 * <p>
 * Please see the documentation for more information.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BsonObject implements Iterable<Map.Entry<String, BsonValue>> {

    private Map<String, BsonValue> map;

    /**
     * Create a new, empty instance
     */
    public BsonObject() {
        map = new LinkedHashMap<>();
    }

    /**
     * Create an instance from a Map. The Map is not copied.
     *
     * @param map the map to create the instance from.
     */
    BsonObject(Map<String, BsonValue> map) {
        this.map = map;
    }

    public boolean isNull(String key) {
        return map.get(key).isNull();
    }

    public boolean isAbsent(String key) {
        return !map.containsKey(key);
    }

    public BsonValue getBsonValue(String key) {
        Objects.requireNonNull(key);
        final BsonValue result = map.get(key);
        return result == null ? BsonValue.nullValue() : result;
    }

    /**
     * Get the string value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not a String
     */
    public String getString(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.stringVisitor);
    }

    /**
     * Get the Integer value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not an Integer
     */
    public Integer getInteger(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.integerVisitor);
    }

    /**
     * Get the Long value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not a Long
     */
    public Long getLong(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.longVisitor);
    }

    /**
     * Get the Double value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not a Double
     */
    public Double getDouble(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.doubleVisitor);
    }

    /**
     * Get the Float value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not a Float
     */
    public Float getFloat(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.floatVisitor);
    }

    /**
     * Get the Boolean value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not a Boolean
     */
    public Boolean getBoolean(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.booleanVisitor);
    }

    /**
     * Get the BsonObject value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not a BsonObject
     */
    public BsonObject getBsonObject(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.bsonObjectVisitor);
    }

    /**
     * Get the BsonArray value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException if the value is not a BsonArray
     */
    public BsonArray getBsonArray(String key) {
        return getBsonValue(key).visit(ClassCastExceptionBsonValueVisitor.bsonArrayVisitor);
    }

    /**
     * Get the binary value with the specified key.
     * <p>
     * JSON itself has no notion of a binary, this extension complies to the RFC-7493, so this method assumes there is a
     * String value with the key and it contains a Base64 encoded binary, which it decodes if found and returns.
     * <p>
     * This method should be used in conjunction with {@link #put(String, byte[])}
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException       if the value is not a String
     * @throws IllegalArgumentException if the String value is not a legal Base64 encoded value
     */
    public byte[] getBinary(String key) {
        final String encoded = getString(key);
        return encoded == null ? null : Base64.getDecoder().decode(encoded);
    }

    /**
     * Get the instant value with the specified key.
     * <p>
     * JSON itself has no notion of a date, this extension complies to the RFC-7493, so this method assumes there is a
     * String value with the key and it contains a ISODATE encoded date, which it decodes if found and returns.
     * <p>
     * This method should be used in conjunction with {@link #put(String, Instant)}
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws ClassCastException       if the value is not a String
     * @throws IllegalArgumentException if the String value is not a legal Base64 encoded value
     */
    public Instant getInstant(String key) {
        final String encoded = getString(key);
        return encoded == null ? null : Instant.from(ISO_INSTANT.parse(encoded));
    }

    /**
     * Like {@link #getString(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public String getString(String key, String def) {
        Objects.requireNonNull(key);
        final String string = getString(key);
        return string != null || map.containsKey(key) ? string : def;
    }

    /**
     * Like {@link #getInteger(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Integer getInteger(String key, Integer def) {
        Objects.requireNonNull(key);
        if (map.containsKey(key)) {
            return getInteger(key);
        } else {
            return def;
        }
    }

    /**
     * Like {@link #getLong(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Long getLong(String key, Long def) {
        Objects.requireNonNull(key);
        if (map.containsKey(key)) {
            return getLong(key);
        } else {
            return def;
        }
    }

    /**
     * Like {@link #getDouble(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Double getDouble(String key, Double def) {
        Objects.requireNonNull(key);
        if (map.containsKey(key)) {
            return getDouble(key);
        } else {
            return def;
        }
    }

    /**
     * Like {@link #getFloat(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Float getFloat(String key, Float def) {
        Objects.requireNonNull(key);
        if (map.containsKey(key)) {
            return getFloat(key);
        } else {
            return def;
        }
    }

    /**
     * Like {@link #getBoolean(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Boolean getBoolean(String key, Boolean def) {
        Objects.requireNonNull(key);
        final Boolean val = getBoolean(key);
        return val != null || map.containsKey(key) ? val : def;
    }

    /**
     * Like {@link #getBsonObject(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public BsonObject getBsonObject(String key, BsonObject def) {
        BsonObject val = getBsonObject(key);
        return val != null || map.containsKey(key) ? val : def;
    }

    /**
     * Like {@link #getBsonArray(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public BsonArray getBsonArray(String key, BsonArray def) {
        BsonArray val = getBsonArray(key);
        return val != null || map.containsKey(key) ? val : def;
    }

    /**
     * Like {@link #getBinary(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public byte[] getBinary(String key, byte[] def) {
        Objects.requireNonNull(key);
        final String val = getString(key);
        return val != null || map.containsKey(key) ? (val == null ? null : Base64.getDecoder().decode(val)) : def;
    }

    /**
     * Like {@link #getInstant(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Instant getInstant(String key, Instant def) {
        Objects.requireNonNull(key);
        final String val = getString(key);
        return val != null || map.containsKey(key) ?
                (val == null ? null : Instant.from(ISO_INSTANT.parse(val))) : def;
    }

    /**
     * Does the JSON object contain the specified key?
     *
     * @param key the key
     * @return true if it contains the key, false if not.
     */
    public boolean containsKey(String key) {
        Objects.requireNonNull(key);
        return map.containsKey(key);
    }

    /**
     * Return the set of field names in the JSON objects
     *
     * @return the set of field names
     */
    public Set<String> fieldNames() {
        return map.keySet();
    }

    /**
     * Put an Enum into the JSON object with the specified key.
     * <p>
     * JSON has no concept of encoding Enums, so the Enum will be converted to a String using the {@link Enum#name}
     * method and the value put as a String.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, Enum value) {
        Objects.requireNonNull(key);
        final String string = value == null ? null : value.name();
        map.put(key, BsonValue.of(string));
        return this;
    }

    public BsonObject put(String key, BsonValue value) {
        Objects.requireNonNull(key);
        map.put(key, value);
        return this;
    }

    /**
     * Put an CharSequence into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, CharSequence value) {

        Objects.requireNonNull(key);
        final String string = value == null ? null : value.toString();
        map.put(key, BsonValue.of(string));
        return this;
    }

    /**
     * Put a String into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, String value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put an Integer into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, Integer value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put a Long into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, Long value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put a Double into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, Double value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put a Float into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, Float value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put a Boolean into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, Boolean value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put a null value into the JSON object with the specified key.
     *
     * @param key the key
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject putNull(String key) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.nullValue());
        return this;
    }

    /**
     * Put another JSON object into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, BsonObject value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put a JSON array into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, BsonArray value) {
        Objects.requireNonNull(key);
        map.put(key, BsonValue.of(value));
        return this;
    }

    /**
     * Put a byte[] into the JSON object with the specified key.
     * <p>
     * JSON extension RFC7493, binary will first be Base64 encoded before being put as a String.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, byte[] value) {
        Objects.requireNonNull(key);
        final String string = value == null ? null : Base64.getEncoder().encodeToString(value);
        map.put(key, BsonValue.of(string));
        return this;
    }

    /**
     * Put a Instant into the JSON object with the specified key.
     * <p>
     * JSON extension RFC7493, instant will first be encoded to ISODATE String.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject put(String key, Instant value) {
        Objects.requireNonNull(key);
        final String string = value == null ? null : ISO_INSTANT.format(value);
        map.put(key, BsonValue.of(string));
        return this;
    }

    /**
     * Remove an entry of this object.
     *
     * @param key the key
     * @return the value that was removed, or null if none
     */
    public BsonValue remove(String key) {
        return map.remove(key);
    }

    /**
     * Merge in another JSON object.
     * <p>
     * This is the equivalent of putting all the entries of the other JSON object into this object.
     *
     * @param other the other JSON object
     * @return a reference to this, so the API can be used fluently
     */
    public BsonObject mergeIn(BsonObject other) {
        map.putAll(other.map);
        return this;
    }

    /**
     * Copy the JSON object
     *
     * @return a copy of the object
     */
    public BsonObject copy() {
        Map<String, BsonValue> copiedMap = new HashMap<>(map.size());
        for (Map.Entry<String, BsonValue> entry : map.entrySet()) {
            copiedMap.put(entry.getKey(), entry.getValue().copy());
        }
        return new BsonObject(copiedMap);
    }

    /**
     * Get the underlying Map.
     *
     * @return the underlying Map.
     */
    public Map<String, BsonValue> getMap() {
        return map;
    }

    /**
     * Get a stream of the entries in the JSON object.
     *
     * @return a stream of the entries.
     */
    public Stream<Map.Entry<String, BsonValue>> stream() {
        return Bson.asStream(iterator());
    }

    /**
     * Get a stream of the keys in the JSON object
     *
     * @return a stream of the keys
     */
    public Stream<String> keyStream() { return stream().map(Map.Entry::getKey); }

    /**
     * Get an Iterator of the entries in the JSON object.
     *
     * @return an Iterator of the entries
     */
    @Override
    public Iterator<Map.Entry<String, BsonValue>> iterator() {
        return map.entrySet().iterator();
    }

    /**
     * Get the number of entries in the JSON object
     *
     * @return the number of entries
     */
    public int size() {
        return map.size();
    }

    /**
     * Remove all the entries in this JSON object
     */
    public BsonObject clear() {
        map.clear();
        return this;
    }

    public static BsonObject from(Stream<KeyVal<String, BsonObject>> iterable) {
        final BsonObject result = new BsonObject();
        iterable.forEach(kv -> result.put(kv.getKey(), kv.getValue()));
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("BsonObject{");

        final List<String> elements = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : map.entrySet()) {
            elements.add(entry.getKey() + ": " + entry.getValue().toString());
        }

        stringBuilder.append(String.join(", ", elements));
        stringBuilder.append("}");

        return stringBuilder.toString();
    }

    /**
     * Is this object entry?
     *
     * @return true if it has zero entries, false if not.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BsonObject entries = (BsonObject) o;
        return Objects.equals(map, entries.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

}
