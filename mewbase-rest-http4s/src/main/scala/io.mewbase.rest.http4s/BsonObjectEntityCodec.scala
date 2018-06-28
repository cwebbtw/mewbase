package io.mewbase.rest.http4s

import cats.Monad
import cats.effect.Sync
import io.mewbase.bson.BsonObject
import io.vertx.core.json.JsonObject
import org.http4s.{DecodeFailure, DecodeResult, EntityDecoder, EntityEncoder, InvalidMessageBodyFailure, MediaType}
import org.http4s.headers.`Content-Type`

import scala.util.Try

object BsonObjectEntityCodec {

  implicit def BsonObjectEntityEncoder[F[_]](implicit F: Monad[F]): EntityEncoder[F, BsonObject] =
    EntityEncoder.stringEncoder.contramap[BsonObject] { obj =>
      obj.encodeToString()
    }.withContentType(`Content-Type`(MediaType.`application/json`))

  implicit def BsonObjectEntityDecoder[F[_]](implicit F: Sync[F]): EntityDecoder[F, BsonObject] =
    EntityDecoder.text.transform { stringDecode =>
      stringDecode.right.flatMap[DecodeFailure, JsonObject] { string =>
        def buildError(throwable: Throwable): DecodeFailure =
          InvalidMessageBodyFailure(throwable.getMessage, Some(throwable))
        Try(new JsonObject(string)).toEither.left.map(buildError)
      }.map(new BsonObject(_))
    }

}
