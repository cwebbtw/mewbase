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
 * Derived from original file JsonArrayTest.java from Vert.x
 */

package io.mewbase.bson;

import io.mewbase.TestUtils;

import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static org.junit.Assert.*;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BsonArrayTest {

    private BsonArray bsonArray;

    @Before
    public void setUp() {
        bsonArray = new BsonArray();
    }

    @Test
    public void testGetInteger() {
        bsonArray.add(123);
        assertEquals(Integer.valueOf(123), bsonArray.getInteger(0));
        try {
            bsonArray.getInteger(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getInteger(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        // Different number types
        bsonArray.add(123l);
        assertEquals(Integer.valueOf(123), bsonArray.getInteger(1));
        bsonArray.add(123f);
        assertEquals(Integer.valueOf(123), bsonArray.getInteger(2));
        bsonArray.add(123d);
        assertEquals(Integer.valueOf(123), bsonArray.getInteger(3));
        bsonArray.add("foo");
        try {
            bsonArray.getInteger(4);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getInteger(5));
    }

    @Test
    public void testGetLong() {
        bsonArray.add(123l);
        assertEquals(Long.valueOf(123l), bsonArray.getLong(0));
        try {
            bsonArray.getLong(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getLong(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        // Different number types
        bsonArray.add(123);
        assertEquals(Long.valueOf(123l), bsonArray.getLong(1));
        bsonArray.add(123f);
        assertEquals(Long.valueOf(123l), bsonArray.getLong(2));
        bsonArray.add(123d);
        assertEquals(Long.valueOf(123l), bsonArray.getLong(3));
        bsonArray.add("foo");
        try {
            bsonArray.getLong(4);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getLong(5));
    }

    @Test
    public void testGetFloat() {
        bsonArray.add(123f);
        assertEquals(Float.valueOf(123f), bsonArray.getFloat(0));
        try {
            bsonArray.getFloat(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getFloat(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        // Different number types
        bsonArray.add(123);
        assertEquals(Float.valueOf(123f), bsonArray.getFloat(1));
        bsonArray.add(123);
        assertEquals(Float.valueOf(123f), bsonArray.getFloat(2));
        bsonArray.add(123d);
        assertEquals(Float.valueOf(123f), bsonArray.getFloat(3));
        bsonArray.add("foo");
        try {
            bsonArray.getFloat(4);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getFloat(5));
    }

    @Test
    public void testGetDouble() {
        bsonArray.add(123d);
        assertEquals(Double.valueOf(123d), bsonArray.getDouble(0));
        try {
            bsonArray.getDouble(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getDouble(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        // Different number types
        bsonArray.add(123);
        assertEquals(Double.valueOf(123d), bsonArray.getDouble(1));
        bsonArray.add(123);
        assertEquals(Double.valueOf(123d), bsonArray.getDouble(2));
        bsonArray.add(123d);
        assertEquals(Double.valueOf(123d), bsonArray.getDouble(3));
        bsonArray.add("foo");
        try {
            bsonArray.getDouble(4);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getDouble(5));
    }

    @Test
    public void testGetString() {
        bsonArray.add("foo");
        assertEquals("foo", bsonArray.getString(0));
        try {
            bsonArray.getString(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getString(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        bsonArray.add(123);
        try {
            bsonArray.getString(1);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getString(2));
    }

    @Test
    public void testGetBoolean() {
        bsonArray.add(true);
        assertEquals(true, bsonArray.getBoolean(0));
        bsonArray.add(false);
        assertEquals(false, bsonArray.getBoolean(1));
        try {
            bsonArray.getBoolean(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getBoolean(2);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        bsonArray.add(123);
        try {
            bsonArray.getBoolean(2);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getBoolean(3));
    }

    @Test
    public void testGetBinary() {
        byte[] bytes = TestUtils.randomByteArray(10);
        bsonArray.add(bytes);
        assertTrue(TestUtils.byteArraysEqual(bytes, bsonArray.getBinary(0)));
        assertTrue(TestUtils.byteArraysEqual(bytes, Base64.getDecoder().decode(bsonArray.getString(0))));
        try {
            bsonArray.getBinary(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getBinary(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        bsonArray.add(123);
        try {
            bsonArray.getBinary(1);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getBinary(2));
    }

    @Test
    public void testGetInstant() {
        Instant now = Instant.now();
        bsonArray.add(now);
        assertEquals(now, bsonArray.getInstant(0));
        assertEquals(now, Instant.from(ISO_INSTANT.parse(bsonArray.getString(0))));
        try {
            bsonArray.getInstant(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getInstant(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        bsonArray.add(123);
        try {
            bsonArray.getInstant(1);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getInstant(2));
    }

    @Test
    public void testGetBsonObject() {
        BsonObject obj = new BsonObject().put("foo", "bar");
        bsonArray.add(obj);
        assertEquals(obj, bsonArray.getBsonObject(0));
        try {
            bsonArray.getBsonObject(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getBsonObject(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        bsonArray.add(123);
        try {
            bsonArray.getBsonObject(1);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getBsonObject(2));
    }

    @Test
    public void testGetBsonArray() {
        BsonArray arr = new BsonArray().add("foo");
        bsonArray.add(arr);
        assertEquals(arr, bsonArray.getBsonArray(0));
        try {
            bsonArray.getBsonArray(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getBsonArray(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        bsonArray.add(123);
        try {
            bsonArray.getBsonArray(1);
            fail();
        } catch (ClassCastException e) {
            // OK
        }
        bsonArray.addNull();
        assertNull(bsonArray.getBsonArray(2));
    }

    @Test
    public void testGetValue() {
        bsonArray.add(123);
        assertEquals(Integer.valueOf(123), bsonArray.getInteger(0));
        bsonArray.add(123L);
        assertEquals(Long.valueOf(123L), bsonArray.getLong(1));
        bsonArray.add(123f);
        assertEquals(Float.valueOf(123f), bsonArray.getFloat(2));
        bsonArray.add(123d);
        assertEquals(Double.valueOf(123d), bsonArray.getDouble(3));
        bsonArray.add(false);
        assertEquals(false, bsonArray.getBoolean(4));
        bsonArray.add(true);
        assertEquals(true, bsonArray.getBoolean(5));
        bsonArray.add("bar");
        assertEquals("bar", bsonArray.getString(6));
        BsonObject obj = new BsonObject().put("blah", "wibble");
        bsonArray.add(obj);
        assertEquals(obj, bsonArray.getBsonObject(7));
        BsonArray arr = new BsonArray().add("blah").add("wibble");
        bsonArray.add(arr);
        assertEquals(arr, bsonArray.getBsonArray(8));
        byte[] bytes = TestUtils.randomByteArray(100);
        bsonArray.add(bytes);
        assertTrue(TestUtils.byteArraysEqual(bytes, Base64.getDecoder().decode((String)bsonArray.getString(9))));
        Instant now = Instant.now();
        bsonArray.add(now);
        assertEquals(now, bsonArray.getInstant(10));
        bsonArray.addNull();
        assertNull(bsonArray.getString(11));
        try {
            bsonArray.getString(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        try {
            bsonArray.getString(12);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // OK
        }
        // BsonObject with inner Map
        List<BsonValue> list = new ArrayList<>();
        Map<String, BsonValue> innerMap = new HashMap<>();
        innerMap.put("blah", BsonValue.of("wibble"));
        list.add(BsonValue.of(innerMap));
        bsonArray = new BsonArray(list);
        obj = bsonArray.getBsonObject(0);
        assertEquals("wibble", obj.getString("blah"));
        // BsonObject with inner List
        list = new ArrayList<>();
        List<BsonValue> innerList = new ArrayList<>();
        innerList.add(BsonValue.of("blah"));
        list.add(BsonValue.of(innerList));
        bsonArray = new BsonArray(list);
        arr = bsonArray.getBsonArray(0);
        assertEquals("blah", arr.getString(0));
    }

    enum SomeEnum {
        FOO, BAR
    }

    @Test
    public void testAddEnum() {
        assertSame(bsonArray, bsonArray.add(BsonObjectTest.SomeEnum.FOO));
        assertEquals(BsonObjectTest.SomeEnum.FOO.toString(), bsonArray.getString(0));
        try {
            bsonArray.add((BsonObjectTest.SomeEnum)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddString() {
        assertSame(bsonArray, bsonArray.add("foo"));
        assertEquals("foo", bsonArray.getString(0));
        try {
            bsonArray.add((String)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddCharSequence() {
        assertSame(bsonArray, bsonArray.add(new StringBuilder("bar")));
        assertEquals("bar", bsonArray.getString(0));
        try {
            bsonArray.add((CharSequence)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddInteger() {
        assertSame(bsonArray, bsonArray.add(123));
        assertEquals(Integer.valueOf(123), bsonArray.getInteger(0));
        try {
            bsonArray.add((Integer)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddLong() {
        assertSame(bsonArray, bsonArray.add(123l));
        assertEquals(Long.valueOf(123l), bsonArray.getLong(0));
        try {
            bsonArray.add((Long)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddFloat() {
        assertSame(bsonArray, bsonArray.add(123f));
        assertEquals(Float.valueOf(123f), bsonArray.getFloat(0));
        try {
            bsonArray.add((Float)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddDouble() {
        assertSame(bsonArray, bsonArray.add(123d));
        assertEquals(Double.valueOf(123d), bsonArray.getDouble(0));
        try {
            bsonArray.add((Double)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddBoolean() {
        assertSame(bsonArray, bsonArray.add(true));
        assertEquals(true, bsonArray.getBoolean(0));
        bsonArray.add(false);
        assertEquals(false, bsonArray.getBoolean(1));
        try {
            bsonArray.add((Boolean)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddBsonObject() {
        BsonObject obj = new BsonObject().put("foo", "bar");
        assertSame(bsonArray, bsonArray.add(obj));
        assertEquals(obj, bsonArray.getBsonObject(0));
        try {
            bsonArray.add((BsonObject)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddBsonArray() {
        BsonArray arr = new BsonArray().add("foo");
        assertSame(bsonArray, bsonArray.add(arr));
        assertEquals(arr, bsonArray.getBsonArray(0));
        try {
            bsonArray.add((BsonArray)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddBinary() {
        byte[] bytes = TestUtils.randomByteArray(10);
        assertSame(bsonArray, bsonArray.add(bytes));
        assertTrue(TestUtils.byteArraysEqual(bytes, bsonArray.getBinary(0)));
        try {
            bsonArray.add((byte[])null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddInstant() {
        Instant now = Instant.now();
        assertSame(bsonArray, bsonArray.add(now));
        assertEquals(now, bsonArray.getInstant(0));
        try {
            bsonArray.add((Instant)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddObject() {
        bsonArray.add("bar");
        bsonArray.add(123);
        bsonArray.add(123L);
        bsonArray.add(1.23f);
        bsonArray.add(1.23d);
        bsonArray.add(true);
        byte[] bytes = TestUtils.randomByteArray(10);
        bsonArray.add(bytes);
        Instant now = Instant.now();
        bsonArray.add(now);
        BsonObject obj = new BsonObject().put("foo", "blah");
        BsonArray arr = new BsonArray().add("quux");
        bsonArray.add(obj);
        bsonArray.add(arr);
        assertEquals("bar", bsonArray.getString(0));
        assertEquals(Integer.valueOf(123), bsonArray.getInteger(1));
        assertEquals(Long.valueOf(123l), bsonArray.getLong(2));
        assertEquals(Float.valueOf(1.23f), bsonArray.getFloat(3));
        assertEquals(Double.valueOf(1.23d), bsonArray.getDouble(4));
        assertEquals(true, bsonArray.getBoolean(5));
        assertTrue(TestUtils.byteArraysEqual(bytes, bsonArray.getBinary(6)));
        assertEquals(now, bsonArray.getInstant(7));
        assertEquals(obj, bsonArray.getBsonObject(8));
        assertEquals(arr, bsonArray.getBsonArray(9));
    }

    @Test
    public void testAddAllBsonArray() {
        bsonArray.add("bar");
        BsonArray arr = new BsonArray().add("foo").add(48);
        assertSame(bsonArray, bsonArray.addAll(arr));
        assertEquals(arr.getString(0), bsonArray.getString(1));
        assertEquals(arr.getInteger(1), bsonArray.getInteger(2));
        try {
            bsonArray.add((BsonArray)null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testAddNull() {
        assertSame(bsonArray, bsonArray.addNull());
        assertEquals(null, bsonArray.getString(0));
        assertTrue(bsonArray.hasNull(0));
    }

    @Test
    public void testHasNull() {
        bsonArray.addNull();
        bsonArray.add("foo");
        assertEquals(null, bsonArray.getString(0));
        assertTrue(bsonArray.hasNull(0));
        assertFalse(bsonArray.hasNull(1));
    }

    @Test
    public void testContains() {
        bsonArray.add("wibble");
        bsonArray.add(true);
        bsonArray.add(123);
        BsonObject obj = new BsonObject();
        BsonArray arr = new BsonArray();
        bsonArray.add(obj);
        bsonArray.add(arr);
        assertFalse(bsonArray.contains(BsonValue.of("eek")));
        assertFalse(bsonArray.contains(BsonValue.of(false)));
        assertFalse(bsonArray.contains(BsonValue.of(321)));
        assertFalse(bsonArray.contains(BsonValue.of(new BsonObject().put("blah", "flib"))));
        assertFalse(bsonArray.contains(BsonValue.of(new BsonArray().add("oob"))));
        assertTrue(bsonArray.contains(BsonValue.of("wibble")));
        assertTrue(bsonArray.contains(BsonValue.of(true)));
        assertTrue(bsonArray.contains(BsonValue.of(123)));
        assertTrue(bsonArray.contains(BsonValue.of(obj)));
        assertTrue(bsonArray.contains(BsonValue.of(arr)));
    }

    @Test
    public void testRemoveByObject() {
        bsonArray.add("wibble");
        bsonArray.add(true);
        bsonArray.add(123);
        assertEquals(3, bsonArray.size());
        assertTrue(bsonArray.remove(BsonValue.of("wibble")));
        assertEquals(2, bsonArray.size());
        assertFalse(bsonArray.remove(BsonValue.of("notthere")));
        assertTrue(bsonArray.remove(BsonValue.of(true)));
        assertTrue(bsonArray.remove(BsonValue.of(123)));
        assertTrue(bsonArray.isEmpty());
    }

    @Test
    public void testRemoveByPos() {
        bsonArray.add("wibble");
        bsonArray.add(true);
        bsonArray.add(123);
        assertEquals(3, bsonArray.size());
        assertEquals(BsonValue.of("wibble"), bsonArray.remove(0));
        assertEquals(2, bsonArray.size());
        assertEquals(BsonValue.of(123), bsonArray.remove(1));
        assertEquals(1, bsonArray.size());
        assertEquals(BsonValue.of(true), bsonArray.remove(0));
        assertTrue(bsonArray.isEmpty());
    }

    @Test
    public void testSize() {
        bsonArray.add("wibble");
        bsonArray.add(true);
        bsonArray.add(123);
        assertEquals(3, bsonArray.size());
    }

    @Test
    public void testClear() {
        bsonArray.add("wibble");
        bsonArray.add(true);
        bsonArray.add(123);
        assertEquals(3, bsonArray.size());
        assertEquals(bsonArray, bsonArray.clear());
        assertEquals(0, bsonArray.size());
        assertTrue(bsonArray.isEmpty());
    }

    @Test
    public void testIterator() {
        bsonArray.add("foo");
        bsonArray.add(123);
        BsonObject obj = new BsonObject().put("foo", "bar");
        bsonArray.add(obj);
        Iterator<BsonValue> iter = bsonArray.iterator();
        assertTrue(iter.hasNext());
        BsonValue entry = iter.next();
        assertEquals(BsonValue.of("foo"), entry);
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals(BsonValue.of(123), entry);
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals(BsonValue.of(obj), entry);
        assertFalse(iter.hasNext());
        iter.remove();
        assertFalse(bsonArray.contains(BsonValue.of(obj)));
        assertEquals(2, bsonArray.size());
    }

    @Test
    public void testStream() {
        bsonArray.add("foo");
        bsonArray.add(123);
        BsonObject obj = new BsonObject().put("foo", "bar");
        bsonArray.add(obj);
        List<BsonValue> list = bsonArray.stream().collect(Collectors.toList());
        Iterator<BsonValue> iter = list.iterator();
        assertTrue(iter.hasNext());
        BsonValue entry = iter.next();
        assertEquals(BsonValue.of("foo"), entry);
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals(BsonValue.of(123), entry);
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals(BsonValue.of(obj), entry);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testCopy() {
        bsonArray.add("foo");
        bsonArray.add(123);
        BsonObject obj = new BsonObject().put("foo", "bar");
        bsonArray.add(obj);
        bsonArray.add(new StringBuilder("eeek"));
        BsonArray copy = bsonArray.copy();
        assertEquals("eeek", copy.getString(3));
        assertNotSame(bsonArray, copy);
        assertEquals(bsonArray, copy);
        assertEquals(4, copy.size());
        assertEquals("foo", copy.getString(0));
        assertEquals(Integer.valueOf(123), copy.getInteger(1));
        assertEquals(obj, copy.getBsonObject(2));
        assertNotSame(obj, copy.getBsonObject(2));
        copy.add("foo");
        assertEquals(4, bsonArray.size());
        bsonArray.add("bar");
        assertEquals(5, copy.size());
    }

    @Test
    public void testGetList() {
        BsonObject obj = new BsonObject().put("quux", "wibble");
        bsonArray.add("foo").add(123).add(obj);
        List<BsonValue> list = bsonArray.getList();
        list.remove(BsonValue.of("foo"));
        assertFalse(bsonArray.contains(BsonValue.of("foo")));
        list.add(BsonValue.of("floob"));
        assertTrue(bsonArray.contains(BsonValue.of("floob")));
        assertEquals(BsonValue.of(obj), list.get(1));
        obj.remove("quux");
    }

    @Test
    public void testCreateFromList() {
        List<BsonValue> list = new ArrayList<>();
        list.add(BsonValue.of("foo"));
        list.add(BsonValue.of(123));
        BsonArray arr = new BsonArray(list);
        assertEquals("foo", arr.getString(0));
        assertEquals(Integer.valueOf(123), arr.getInteger(1));
        assertSame(list, arr.getList());
    }

    @Test
    public void testCreateFromListCharSequence() {
        List<BsonValue> list = new ArrayList<>();
        list.add(BsonValue.of("foo"));
        list.add(BsonValue.of(123));
        list.add(BsonValue.of(new StringBuilder("eek")));
        BsonArray arr = new BsonArray(list);
        assertEquals("foo", arr.getString(0));
        assertEquals(Integer.valueOf(123), arr.getInteger(1));
        assertEquals("eek", arr.getString(2));
        assertSame(list, arr.getList());
    }

    @Test
    public void testCreateFromListNestedBsonObject() {
        List<BsonValue> list = new ArrayList<>();
        list.add(BsonValue.of("foo"));
        list.add(BsonValue.of(123));
        BsonObject obj = new BsonObject().put("blah", "wibble");
        list.add(BsonValue.of(obj));
        BsonArray arr = new BsonArray(list);
        assertEquals("foo", arr.getString(0));
        assertEquals(Integer.valueOf(123), arr.getInteger(1));
        assertSame(list, arr.getList());
        assertSame(obj, arr.getBsonObject(2));
    }

    @Test
    public void testCreateFromListNestedMap() {
        List<BsonValue> list = new ArrayList<>();
        list.add(BsonValue.of("foo"));
        list.add(BsonValue.of(123));
        Map<String, BsonValue> map = new HashMap<>();
        map.put("blah", BsonValue.of("wibble"));
        list.add(BsonValue.of(map));
        BsonArray arr = new BsonArray(list);
        assertEquals("foo", arr.getString(0));
        assertEquals(Integer.valueOf(123), arr.getInteger(1));
        assertSame(list, arr.getList());
        BsonObject obj = arr.getBsonObject(2);
        assertSame(map, obj.getMap());
    }

    @Test
    public void testCreateFromListNestedBsonArray() {
        List<BsonValue> list = new ArrayList<>();
        list.add(BsonValue.of("foo"));
        list.add(BsonValue.of(123));
        BsonArray arr2 = new BsonArray().add("blah").add("wibble");
        list.add(BsonValue.of(arr2));
        BsonArray arr = new BsonArray(list);
        assertEquals("foo", arr.getString(0));
        assertEquals(Integer.valueOf(123), arr.getInteger(1));
        assertSame(list, arr.getList());
        assertSame(arr2, arr.getBsonArray(2));
    }

    @Test
    public void testCreateFromListNestedList() {
        List<BsonValue> list = new ArrayList<>();
        list.add(BsonValue.of("foo"));
        list.add(BsonValue.of(123));
        List<BsonValue> list2 = new ArrayList<>();
        list2.add(BsonValue.of("blah"));
        list2.add(BsonValue.of("wibble"));
        list.add(BsonValue.of(list2));
        BsonArray arr = new BsonArray(list);
        assertEquals("foo", arr.getString(0));
        assertEquals(Integer.valueOf(123), arr.getInteger(1));
        assertSame(list, arr.getList());
        BsonArray arr2 = arr.getBsonArray(2);
        assertSame(list2, arr2.getList());
    }

    @Test
    public void testBsonArrayEquality() {
        BsonObject obj = new BsonObject(Collections.singletonMap("abc", BsonValue.of(Collections.singletonList(BsonValue.of(3)))));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", BsonValue.of(Collections.singletonList(BsonValue.of(3))))));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", BsonValue.of(Collections.singletonList(BsonValue.of(3L))))));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", BsonValue.of(new BsonArray().add(3)))));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", BsonValue.of(new BsonArray().add(3L)))));
        assertNotEquals(obj, new BsonObject(Collections.singletonMap("abc", BsonValue.of(Collections.singletonList(BsonValue.of(4))))));
        assertNotEquals(obj, new BsonObject(Collections.singletonMap("abc", BsonValue.of(new BsonArray().add(4)))));
        BsonArray array = new BsonArray(Collections.singletonList(BsonValue.of(Collections.singletonList(BsonValue.of(3)))));
        assertEquals(array, new BsonArray(Collections.singletonList(BsonValue.of(Collections.singletonList(BsonValue.of(3))))));
        assertEquals(array, new BsonArray(Collections.singletonList(BsonValue.of(Collections.singletonList(BsonValue.of(3L))))));
        assertEquals(array, new BsonArray(Collections.singletonList(BsonValue.of(new BsonArray().add(3)))));
        assertEquals(array, new BsonArray(Collections.singletonList(BsonValue.of(new BsonArray().add(3L)))));
        assertNotEquals(array, new BsonArray(Collections.singletonList(BsonValue.of(Collections.singletonList(BsonValue.of(4))))));
        assertNotEquals(array, new BsonArray(Collections.singletonList(BsonValue.of(new BsonArray().add(4)))));
    }

    @Test
    public void testStreamCorrectTypes() {
        BsonObject object = new BsonObject();
        object.put("object1", new BsonArray().add(new BsonObject().put("object2", 12)));
        testStreamCorrectTypes(object.copy());
        testStreamCorrectTypes(object);
    }

    @Test
    public void testRemoveMethodReturnedObject() {
        BsonArray obj = new BsonArray();
        obj.add("bar")
                .add(new BsonObject().put("name", "vert.x").put("count", 2))
                .add(new BsonArray().add(1.0).add(2.0));

        BsonValue removed = obj.remove(0);
        assertTrue(removed instanceof BsonValue.StringBsonValue);

        removed = obj.remove(0);
        assertTrue(removed instanceof BsonValue.BsonObjectBsonValue);
        assertEquals(((BsonValue.BsonObjectBsonValue)removed).getValue().getString("name"), "vert.x");

        removed = obj.remove(0);
        assertTrue(removed instanceof BsonValue.BsonArrayBsonValue);
        assertEquals(((BsonValue.BsonArrayBsonValue)removed).getValue().getDouble(0), 1.0, 0.0);
    }

    private void testStreamCorrectTypes(BsonObject object) {
        object.getBsonArray("object1").stream().forEach(innerMap -> {
            assertTrue("Expecting BsonObject, found: " + innerMap.getClass().getCanonicalName(), innerMap instanceof BsonValue.BsonObjectBsonValue);
        });
    }

}
