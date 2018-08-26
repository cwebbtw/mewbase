package example.gettingstarted

import io.mewbase.Result
import io.mewbase.bson.BsonValue
import io.mewbase.bson.syntax._

sealed trait Action {
  def bsonValue: BsonValue
}
object Action {
  def apply(stringValue: String): Result[Action] =
    stringValue match {
      case Buy.stringValue    => Right(Buy)
      case Refund.stringValue => Right(Refund)
      case _                  => Left("Unknown action " + stringValue)
    }
}

case object Buy extends Action {
  val stringValue: String = "BUY"

  override def toString: String = stringValue

  override def bsonValue: BsonValue = stringValue.bsonValue
}

case object Refund extends Action {
  val stringValue: String = "REFUND"

  override def toString: String = stringValue

  override def bsonValue: BsonValue = stringValue.bsonValue
}
