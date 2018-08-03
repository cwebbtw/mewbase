package io.mewbase.rest.http4s

import cats.effect.Effect
import io.mewbase.rest.RestServiceAction
import org.http4s.Response

import scala.language.higherKinds

trait MewbaseSupport {

  implicit def mewbaseServiceActionToResponse[F[_]: Effect](serviceAction: RestServiceAction[_]): F[Response[F]] = {
    val serviceActionVisitor = new Http4sRestServiceActionVisitor[F]
    serviceAction.visit(serviceActionVisitor)
  }

}
