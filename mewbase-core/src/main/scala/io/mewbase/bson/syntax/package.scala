package io.mewbase.bson

import io.mewbase
import io.mewbase.{Result, bson}
import io.mewbase.bson.BsonValue.StringBsonValue

import scala.reflect.ClassTag

package object syntax {

  def bsonObject(fields: (String, BsonValue)*): BsonObject = {
    val result = new BsonObject()
    fields.foreach {
      case (fieldName, fieldValue) =>
        result.put(fieldName, fieldValue)
    }
    result
  }

  def bsonArray(values: BsonValue*): BsonArray = {
    val result = new BsonArray()
    values.foreach(result.add)
    result
  }

  implicit class BsonObjectSyntax(val bsonObject: BsonObject) {

    def getOrBsonNull(key: String): BsonValue =
      bsonObject.getBsonValue(key)

    def apply(key: String): Option[BsonValue] = {
      val value = bsonObject.getBsonValue(key)

      if (value.isNull)
        None
      else
        Some(value)
    }

    def at(key: String): Result[BsonValue] =
      apply(key).toRight(s"Missing field '$key'")

  }

  implicit class BsonArraySyntax(val bsonArray: BsonArray) {

    def apply(index: Int): Option[BsonValue] =
      if (index < bsonArray.size())
        Some(bsonArray.getBsonValue(index))
      else
        None

  }

  sealed trait LiftBsonValue[T] {
    def lift(value: T): BsonValue
  }

  object LiftBsonValue {
    def apply[T](f: T => BsonValue): LiftBsonValue[T] = new LiftBsonValue[T] {
      override def lift(value: T): BsonValue = f(value)
    }
  }

  implicit val LiftStringBsonValue: LiftBsonValue[String] = LiftBsonValue[String](BsonValue.of)
  implicit val LiftIntBsonValue: LiftBsonValue[Int] = LiftBsonValue[Int](i => BsonValue.of(i.toLong))
  implicit val LiftLongBsonValue: LiftBsonValue[Long] = LiftBsonValue[Long](BsonValue.of)
  implicit val LiftDoubleBsonValue: LiftBsonValue[Double] = LiftBsonValue[Double](BsonValue.of)
  implicit val LiftFloatBsonValue: LiftBsonValue[Float] = LiftBsonValue[Float](f => BsonValue.of(f.toDouble))

  implicit val LiftBooleanBsonValue: LiftBsonValue[Boolean] = LiftBsonValue[Boolean](BsonValue.of)

  implicit val LiftBsonObjectBsonValue: LiftBsonValue[BsonObject] = LiftBsonValue[BsonObject](BsonValue.of)

  implicit val LiftBsonArrayBsonValue: LiftBsonValue[BsonArray] = LiftBsonValue[BsonArray](BsonValue.of)

  implicit val LiftBigDecimalBsonValue: LiftBsonValue[java.math.BigDecimal] = LiftBsonValue[java.math.BigDecimal](BsonValue.of)

  implicit def LiftOptionBsonValue[T](implicit liftDefined: LiftBsonValue[T]): LiftBsonValue[Option[T]] =
    new LiftBsonValue[Option[T]] {
      override def lift(value: Option[T]): BsonValue =
        value match {
          case Some(v) => liftDefined.lift(v)
          case None => BsonValue.nullValue()
        }
    }

  implicit class LiftBsonValueSyntax[T](val value: T) {

    def bsonValue(implicit liftBsonValue: LiftBsonValue[T]): BsonValue =
      liftBsonValue.lift(value)

  }

  sealed trait BsonValueDecoder[T] {
    def decode(value: BsonValue): Result[T]
  }

  object BsonValueDecoder {
    class WrongType[T](implicit classTag: ClassTag[T]) extends BsonValue.Visitor[Result[T]] {
      def result(value: BsonValue): Result[T] =
        Left(s"Unable to decode '$value' into a ${classTag.toString()}")

      override def visit(nullValue: BsonValue.NullBsonValue): Result[T] = result(nullValue)
      override def visit(value: StringBsonValue): Result[T] = result(value)
      override def visit(value: BsonValue.BigDecimalBsonValue): Result[T] = result(value)
      override def visit(value: BsonValue.BooleanBsonValue): Result[T] = result(value)
      override def visit(value: BsonValue.BsonObjectBsonValue): Result[T] = result(value)
      override def visit(value: BsonValue.BsonArrayBsonValue): Result[T] = result(value)
    }

    def apply[T](visitor: BsonValue.Visitor[Result[T]]): BsonValueDecoder[T] =
      new BsonValueDecoder[T] {
        override def decode(value: BsonValue): Result[T] =
          value.visit(visitor)
      }
  }

  implicit val StringBsonValueDecoder: BsonValueDecoder[String] = BsonValueDecoder[String] {
    new bson.syntax.BsonValueDecoder.WrongType[String] {
      override def visit(value: StringBsonValue): Result[String] =
        Right(value.getValue)
    }
  }

  implicit val IntBsonValueDecoder: BsonValueDecoder[Int] = BsonValueDecoder[Int] {
    new mewbase.bson.syntax.BsonValueDecoder.WrongType[Int] {
      override def visit(value: BsonValue.BigDecimalBsonValue): Result[Int] =
        Right(value.getValue.intValue())
    }
  }

  implicit val LongBsonValueDecoder: BsonValueDecoder[Long] = BsonValueDecoder[Long] {
    new mewbase.bson.syntax.BsonValueDecoder.WrongType[Long] {
      override def visit(value: BsonValue.BigDecimalBsonValue): Result[Long] =
        Right(value.getValue.longValue())
    }
  }

  implicit val FloatBsonValueDecoder: BsonValueDecoder[Float] = BsonValueDecoder[Float] {
    new mewbase.bson.syntax.BsonValueDecoder.WrongType[Float] {
      override def visit(value: BsonValue.BigDecimalBsonValue): Result[Float] =
        Right(value.getValue.floatValue())
    }
  }

  implicit val DoubleBsonValueDecoder: BsonValueDecoder[Double] = BsonValueDecoder[Double] {
    new mewbase.bson.syntax.BsonValueDecoder.WrongType[Double] {
      override def visit(value: BsonValue.BigDecimalBsonValue): Result[Double] =
        Right(value.getValue.doubleValue())
    }
  }

  implicit val BooleanBsonValueDecoder: BsonValueDecoder[Boolean] = BsonValueDecoder[Boolean] {
    new mewbase.bson.syntax.BsonValueDecoder.WrongType[Boolean] {
      override def visit(value: BsonValue.BooleanBsonValue): Result[Boolean] =
        Right(value.getValue)
    }
  }

  implicit val BsonObjectBsonValueDecoder: BsonValueDecoder[BsonObject] = BsonValueDecoder[BsonObject] {
    new mewbase.bson.syntax.BsonValueDecoder.WrongType[BsonObject] {
      override def visit(value: BsonValue.BsonObjectBsonValue): Result[BsonObject] =
        Right(value.getValue)
    }
  }

  implicit val BsonArrayBsonValueDecoder: BsonValueDecoder[BsonArray] = BsonValueDecoder[BsonArray] {
    new mewbase.bson.syntax.BsonValueDecoder.WrongType[BsonArray] {
      override def visit(value: BsonValue.BsonArrayBsonValue): Result[BsonArray] =
        Right(value.getValue)
    }
  }

  implicit class BsonValueSyntax(val bsonValue: BsonValue) {

    def to[T](implicit decoder: BsonValueDecoder[T]): Result[T] =
      decoder.decode(bsonValue)

  }
}
