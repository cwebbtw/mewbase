package example.bson

import io.mewbase.bson.{BsonObject, BsonPrisms}
import io.mewbase.bson.syntax._
import monocle.Prism
import monocle.syntax.apply._

/*
This example shows Mewbase's Scala DSL for BSON objects
and how to manipulate bson value instances using prisms
 */
object Main extends App with BsonPrisms {

   val alice =
     bsonObject(
       "name" -> "alice".bsonValue,
       "age" -> 25.bsonValue
     )

  val frank =
    bsonObject(
      "name" -> "Frank".bsonValue,
      "age" -> 50.bsonValue
    )

  val bob =
    bsonObject(
      "name" -> "Bob".bsonValue,
      "age" -> 30.bsonValue,
      "friends" -> bsonArray(
        alice.bsonValue,
        frank.bsonValue
      ).bsonValue
    )

  val frankPrism =
    (bsonObjectPrism
      composeOptional index("friends")
      composePrism bsonArrayPrism
      composeOptional index(1))

  /*
  Some(BsonObjectBsonValue{value=BsonObject{name: StringBsonValue{value='Frank'}, age: BigDecimalBsonValue{value=50}}})
   */
  println(frankPrism.getOption(bob.bsonValue))

  val frankAgePrism =
    (frankPrism
      composePrism bsonObjectPrism
      composeOptional index("age")
      composePrism bsonNumberPrism)


  val olderFrank =
    frankAgePrism.set(new java.math.BigDecimal(51))(bob.bsonValue)

  /*
  Some(BsonObjectBsonValue{value=BsonObject{name: StringBsonValue{value='Frank'}, age: BigDecimalBsonValue{value=51}}})
 */
  println(frankPrism.getOption(olderFrank.bsonValue))



  /*
  BsonObject{a: StringBsonValue{value='hello'}, b: BigDecimalBsonValue{value=123}}
   */
  println(bsonObject() applyLens at("a") set Some("hello".bsonValue) applyLens at("b") set Some(123.bsonValue))

}
