package io.mewbase.rest.http4s

import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier

import cats.effect.Effect
import io.mewbase.bson.{BsonArray, BsonObject}
import io.mewbase.rest.RestServiceAction
import io.mewbase.rest.http4s.BsonEntityCodec._
import org.http4s.{EntityEncoder, Response}
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

class Http4sRestServiceActionVisitor[F[_]: Effect] extends RestServiceAction.Visitor[F[Response[F]]] with Http4sDsl[F] {

  private def okOrISE[T](result: => CompletableFuture[T])(implicit e: EntityEncoder[F, T]): F[Response[F]] =
    okOrISE(result.thenApply[F[Response[F]]](Ok.apply(_)))

  private def okOrISE[T](result: => CompletableFuture[F[Response[F]]]): F[Response[F]] =
    Try(result.get()) match {
      case Success(value) => value
      case Failure(reason) => InternalServerError(reason.getMessage)
    }

  override def visit(retrieveSingleDocument: RestServiceAction.RetrieveSingleDocument): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      val futureDocument: CompletableFuture[Optional[BsonObject]] =
        retrieveSingleDocument.perform()
      val result =
        futureDocument.thenApply[F[Response[F]]](opt => opt.map[F[Response[F]]](Ok.apply(_)).orElseGet(() => NotFound("Could not retrieve document")))

      okOrISE(result)
    }

  override def visit(executeCommand: RestServiceAction.ExecuteCommand): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      val eventId: CompletableFuture[java.lang.Long] = executeCommand.perform()
      val result: CompletableFuture[String] = eventId.thenApplyAsync(_ => "")
      okOrISE(result)
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

  override def visit(getMetrics: RestServiceAction.GetMetrics): F[Response[F]] =
    implicitly[Effect[F]].suspend {
      Ok(getMetrics.perform())
    }
}