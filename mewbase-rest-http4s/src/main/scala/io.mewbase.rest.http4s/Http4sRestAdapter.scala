package io.mewbase.rest.http4s

import java.util.function
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import cats.Monad
import cats.effect.Effect
import io.mewbase.bson.{BsonArray, BsonObject}
import io.mewbase.rest.{RestAdapter, RestServiceAction}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpService, Request, Response}
import io.mewbase.rest.http4s.BsonEntityCodec._

class Http4sRestServiceActionVisitor[F[_]: Effect] extends RestServiceAction.Visitor[F[Response[F]]] with Http4sDsl[F] {

  override def visit(retrieveSingleDocument: RestServiceAction.RetrieveSingleDocument): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      val futureDocument = retrieveSingleDocument.perform()
      val document = futureDocument.get()
      Ok(document)
    }

  override def visit(executeCommand: RestServiceAction.ExecuteCommand): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      executeCommand.perform()
      Ok()
    }

  override def visit(listDocumentIds: RestServiceAction.ListDocumentIds): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      val documentIds = listDocumentIds.perform()
      Ok(BsonArray.from(documentIds))
    }

  override def visit(listBinders: RestServiceAction.ListBinders): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      val binders = listBinders.perform()
      Ok(BsonArray.from(binders))
    }

  override def visit(runQuery: RestServiceAction.RunQuery): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      val result = runQuery.perform()

      if (result.isPresent)
        Ok(result.get())
      else
        NotFound("Query not found")
    }

}

class Http4sRestAdapter[F[_]: Effect](implicit Monad: Monad[F]) extends RestAdapter[Request[F], F[RestServiceAction[_]], HttpService[F]] with Http4sDsl[F] {

  private[this] val actionVisitor =
    new RestServiceAction.Visitor[F[Response[F]]] {

      override def visit(retrieveSingleDocument: RestServiceAction.RetrieveSingleDocument): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val futureDocument = retrieveSingleDocument.perform()
          val document = futureDocument.get()
          Ok(document)
        }

      override def visit(executeCommand: RestServiceAction.ExecuteCommand): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          executeCommand.perform()
          Ok()
        }

      override def visit(listDocumentIds: RestServiceAction.ListDocumentIds): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val documentIds = listDocumentIds.perform()
          Ok(BsonArray.from(documentIds))
        }

      override def visit(listBinders: RestServiceAction.ListBinders): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val binders = listBinders.perform()
          Ok(BsonArray.from(binders))
        }

      override def visit(runQuery: RestServiceAction.RunQuery): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val result = runQuery.perform()

          if (result.isPresent)
            Ok(result.get())
          else
            NotFound("Query not found")
        }
    }

  override def adapt(requestMapper: function.Function[Request[F], F[RestServiceAction[_]]]): HttpService[F] =
    HttpService[F] {
      case request =>
        Monad.flatMap(requestMapper(request))(_.visit(actionVisitor))
    }

}
