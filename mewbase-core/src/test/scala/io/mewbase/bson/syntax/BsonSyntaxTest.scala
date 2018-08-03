package io.mewbase.bson.syntax

import io.mewbase.bson.{BsonArray, BsonObject}
import io.mewbase.bson.BsonValue._
import org.scalatest.{FunSuite, Matchers}

class BsonObjectSyntaxTest extends FunSuite with Matchers {

  test("get missing value results in an empty option") {
    val bsonObject = new BsonObject()

    bsonObject("name") shouldBe None
  }

  test("get a present value results in a defined option") {
    val bsonObject = new BsonObject()

    bsonObject.put("name", "Martin")

    bsonObject("name") shouldBe 'defined
  }

  test("build bson object") {
    val obj =
      bsonObject("hello" -> "world".bsonValue, "count" -> 1.bsonValue)

    obj("hello").map(_.to[String]) shouldBe Some(Right("world"))
  }

}

class BsonArraySyntaxTest extends FunSuite with Matchers {

  test("build bson array") {
    val arr = bsonArray(123.bsonValue, "hello".bsonValue)
    arr(1) shouldBe Some("hello".bsonValue)
  }

  test("get index out of bounds") {
    val bsonArray = new BsonArray()
    bsonArray(0) shouldBe None
  }

}

class BsonValueSyntaxTest extends FunSuite with Matchers {

  test("lift a String") {
    "Hello, world".bsonValue shouldBe a[StringBsonValue]
  }

  test("lift a Double") {
    3.14d.bsonValue shouldBe a[BigDecimalBsonValue]
  }


  test("lift a Long") {
    1234L.bsonValue shouldBe a[BigDecimalBsonValue]
  }

  test("lift an Int") {
    3.bsonValue shouldBe a[BigDecimalBsonValue]
  }

  test("lift a Float") {
    3.14f.bsonValue shouldBe a[BigDecimalBsonValue]
  }

  test("lift a boolean") {
    true.bsonValue shouldBe a[BooleanBsonValue]
  }

  test("lift a bsonobject") {
    val bsonObject = new BsonObject()
    bsonObject.put("hello", "world")
    bsonObject.bsonValue shouldBe a [BsonObjectBsonValue]
  }

  test("lift a bsonarray") {
    val bsonArray = new BsonArray()
    bsonArray.add("hello")
    bsonArray.bsonValue shouldBe a [BsonArrayBsonValue]
  }

  test("lift an undefined option") {
    val value: Option[String] = None
    value.bsonValue shouldBe a [NullBsonValue]
  }

  test("lift a defined option") {
    val value: Option[String] = Some("Hello")
    value.bsonValue shouldBe a [StringBsonValue]
  }

  test("decode a string") {
    "hello".bsonValue.to[String] shouldBe Right("hello")
  }
  test("decode an int to a string") {
    123.bsonValue.to[String] shouldBe a[Left[_, _]]
  }

  test("decode an int") {
    123.bsonValue.to[Int] shouldBe Right(123)
  }

  test("decode a long") {
    123L.bsonValue.to[Long] shouldBe Right(123L)
  }

  test("decode a Double") {
    123d.bsonValue.to[Double] shouldBe Right(123d)
  }

  test("decode a Float") {
    123f.bsonValue.to[Float] shouldBe Right(123f)
  }

  test("decode a boolean") {
    true.bsonValue.to[Boolean] shouldBe Right(true)
  }

  test("decode a bsonobject") {
    new BsonObject().bsonValue.to[BsonObject] shouldBe Right(new BsonObject())
  }

  test("decode a bsonarray") {
    new BsonArray().bsonValue.to[BsonArray] shouldBe Right(new BsonArray())
  }

}