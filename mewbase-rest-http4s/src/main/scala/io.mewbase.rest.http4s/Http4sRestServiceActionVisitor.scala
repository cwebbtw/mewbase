package io.mewbase.rest.http4s

import cats.effect.Effect
import io.mewbase.bson.BsonArray
import io.mewbase.rest.RestServiceAction
import io.mewbase.rest.http4s.BsonEntityCodec._
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

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