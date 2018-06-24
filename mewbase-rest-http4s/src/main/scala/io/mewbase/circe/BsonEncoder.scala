package io.mewbase.circe

import io.circe.{Encoder, Json}
import io.mewbase.bson.{Bson, BsonObject}
import io.circe.parser._

object BsonEncoder {

  implicit val bsonEncoder: Encoder[BsonObject] =
    Encoder.encoderContravariant.contramap(Encoder.encodeJson) { bson =>
      parse(bson.encodeToString()).right.get
    }

}
