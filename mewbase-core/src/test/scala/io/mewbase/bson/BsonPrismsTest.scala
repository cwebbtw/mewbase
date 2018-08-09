package io.mewbase.bson

import org.scalatest.{FunSuite, Matchers, OptionValues}
import io.mewbase.bson.syntax._

class BsonPrismsTest extends FunSuite with BsonPrisms with OptionValues with Matchers {

  test("number prism") {
    bsonNumberPrism.getOption(123.bsonValue).value shouldBe new java.math.BigDecimal(123)
    bsonNumberPrism.getOption("hello".bsonValue) shouldBe None
  }

  test("string prism") {
    bsonStringPrism.getOption("hello".bsonValue).value shouldBe "hello"
    bsonStringPrism.getOption(123.bsonValue) shouldBe None
  }

  test("array prism") {
    bsonArrayPrism.getOption(bsonArray().bsonValue).value shouldBe bsonArray()
    bsonArrayPrism.getOption(bsonObject().bsonValue) shouldBe None
  }

  test("object prism") {
    bsonObjectPrism.getOption(bsonObject().bsonValue).value shouldBe bsonObject()
    bsonObjectPrism.getOption(bsonArray().bsonValue) shouldBe None
  }

  test("numeric value from object") {
    val person =
      bsonObject(
        "name" -> "Bob".bsonValue,
        "age" -> 30.bsonValue
      ).bsonValue

    val age = (bsonObjectPrism
                composeOptional index("age")
                composePrism bsonNumberPrism)

    age.getOption(person).map(_.intValue()) shouldBe Some(30)
  }

  test("add value to array") {
    val person =
      bsonObject(
        "name" -> "Bob".bsonValue,
        "age" -> 30.bsonValue,
        "friends" -> bsonArray(
          bsonObject(
            "name" -> "Alice".bsonValue,
            "age" -> 27.bsonValue
          ).bsonValue
        ).bsonValue
      ).bsonValue

    val secondFriend =
      (bsonObjectPrism
        composeOptional index("friends")
        composePrism bsonArrayPrism
        composeOptional index(0)
        composePrism bsonObjectPrism)

    val newFriend =
      bsonObject(
        "name" -> "Fred".bsonValue,
        "age" -> 25.bsonValue
      )

    val result = secondFriend.set(newFriend)(person)

    person shouldBe person
    result shouldBe
      bsonObject(
        "name" -> "Bob".bsonValue,
        "age" -> 30.bsonValue,
        "friends" -> bsonArray(
          newFriend.bsonValue
        ).bsonValue
      ).bsonValue
  }

}