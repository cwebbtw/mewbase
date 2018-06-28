package io.mewbase.rest.http4s

import java.util.function

import cats.Monad
import cats.effect.Effect
import io.mewbase.rest.{RestAdapter, RestServiceAction}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpService, Request, Response}
import io.mewbase.rest.http4s.BsonObjectEntityCodec._

class Http4sRestAdapter[F[_]: Effect](implicit Monad: Monad[F]) extends RestAdapter[Request[F], F[RestServiceAction], HttpService[F]] with Http4sDsl[F] {

  private[this] val actionVisitor =
    new RestServiceAction.Visitor[F[Response[F]]] {

      override def visit(retrieveSingleDocument: RestServiceAction.RetrieveSingleDocument): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val binder = retrieveSingleDocument.getBinderStore.open(retrieveSingleDocument.getBinderName)
          val futureDocument = binder.get(retrieveSingleDocument.getDocumentId)
          val document = futureDocument.get()
          Ok.apply(document)
        }

      override def visit(executeCommand: RestServiceAction.ExecuteCommand): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          executeCommand.getCommandManager.execute(executeCommand.getCommandName, executeCommand.getContext)
          Ok()
        }
    }

  override def adapt(requestMapper: function.Function[Request[F], F[RestServiceAction]]): HttpService[F] =
    HttpService[F] {
      case request =>
        Monad.flatMap(requestMapper(request))(_.visit(actionVisitor))
    }

}
