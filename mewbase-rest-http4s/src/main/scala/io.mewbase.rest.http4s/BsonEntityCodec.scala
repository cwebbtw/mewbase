package io.mewbase.rest.http4s

import cats.Monad
import cats.effect.Sync
import io.mewbase.bson.{BsonArray, BsonCodec, BsonObject}
import io.vertx.core.json.JsonArray
import org.http4s.{DecodeFailure, EntityDecoder, EntityEncoder, InvalidMessageBodyFailure, MediaType}
import org.http4s.headers.`Content-Type`

object BsonEntityCodec {

  implicit def BsonObjectEntityEncoder[F[_]](implicit F: Monad[F]): EntityEncoder[F, BsonObject] =
    EntityEncoder.stringEncoder.contramap[BsonObject] { obj =>
      BsonCodec.bsonObjectToJsonObject(obj).toString
    }.withContentType(`Content-Type`(MediaType.`application/json`))

  implicit def BsonObjectEntityDecoder[F[_]](implicit F: Sync[F]): EntityDecoder[F, BsonObject] =
    EntityDecoder.text.transform { stringDecode =>
      stringDecode.map(BsonCodec.jsonStringToBsonObject)
    }

  implicit def BsonArrayEncoder[F[_]](implicit F: Monad[F]): EntityEncoder[F, BsonArray] =
    EntityEncoder.stringEncoder.contramap[BsonArray] { arr =>
      BsonCodec.bsonArrayToJsonArray(arr).toString
    }.withContentType(`Content-Type`(MediaType.`application/json`))

  implicit def BsonArrayDecoder[F[_]](implicit F: Sync[F]): EntityDecoder[F, BsonArray] =
    EntityDecoder.text.transform { stringDecode =>
      stringDecode.map(BsonCodec.jsonStringToBsonArray)
    }

}
