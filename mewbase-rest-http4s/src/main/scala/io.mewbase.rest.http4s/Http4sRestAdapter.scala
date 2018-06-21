package io.mewbase.rest.http4s

import java.util.function

import cats.effect.Effect
import io.mewbase.rest.{RestAdapter, RestServiceAction}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpService, Request, Response}

class Http4sRestAdapter[F[_] : Effect] extends RestAdapter[Request[F], HttpService[F]] with Http4sDsl[F] {

  private[this] val actionVisitor =
    new RestServiceAction.Visitor[F[Response[F]]] {
      override def visit(retrieveSingleDocument: RestServiceAction.RetrieveSingleDocument): F[Response[F]] =
        Ok("single doc")
    }

  override def adapt(requestMapper: function.Function[Request[F], RestServiceAction]): HttpService[F] = HttpService[F] {
    case request =>
      val action = requestMapper(request)
      action.visit(actionVisitor)
  }

}
