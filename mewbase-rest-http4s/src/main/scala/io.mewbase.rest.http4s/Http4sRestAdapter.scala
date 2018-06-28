package io.mewbase.rest.http4s

import java.util.function
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import cats.Monad
import cats.effect.Effect
import io.mewbase.binders.KeyVal
import io.mewbase.bson.{BsonArray, BsonObject}
import io.mewbase.rest.{RestAdapter, RestServiceAction}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpService, Request, Response}
import io.mewbase.rest.http4s.BsonEntityCodec._

class Http4sRestAdapter[F[_]: Effect](implicit Monad: Monad[F]) extends RestAdapter[Request[F], F[RestServiceAction], HttpService[F]] with Http4sDsl[F] {

  private[this] val actionVisitor =
    new RestServiceAction.Visitor[F[Response[F]]] {

      override def visit(retrieveSingleDocument: RestServiceAction.RetrieveSingleDocument): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val binder = retrieveSingleDocument.getBinderStore.open(retrieveSingleDocument.getBinderName)
          val futureDocument = binder.get(retrieveSingleDocument.getDocumentId)
          val document = futureDocument.get()
          Ok(document)
        }

      override def visit(executeCommand: RestServiceAction.ExecuteCommand): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          executeCommand.getCommandManager.execute(executeCommand.getCommandName, executeCommand.getContext)
          Ok()
        }

      override def visit(listDocumentIds: RestServiceAction.ListDocumentIds): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val binder = listDocumentIds.getBinderStore.open(listDocumentIds.getBinderName)
          val documents = binder.getDocuments().collect(Collectors.toList[KeyVal[String, _]]).asScala
          val result = new BsonArray()

          documents.foreach { kv =>
            result.add(kv.getKey)
          }

          Ok(result)
        }

      override def visit(listBinders: RestServiceAction.ListBinders): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val binders = listBinders.getBinderStore.binderNames().collect(Collectors.toList[String]).asScala
          val result = new BsonArray()

          binders.foreach(result.add)

          Ok(result)
        }

      override def visit(runQuery: RestServiceAction.RunQuery): F[Response[F]] =
        implicitly[Effect[F]].suspend {
          val queryOpt = runQuery.getQueryManager.getQuery(runQuery.getQueryName)

          if (queryOpt.isPresent) {
            val query = queryOpt.get()

            val documents = query.execute(runQuery.getContext)

            val response = new BsonObject()
            documents.forEach(kv => response.put(kv.getKey, kv.getValue))

            Ok(response)
          }
          else {
            NotFound("Query not found")
          }
        }
    }

  override def adapt(requestMapper: function.Function[Request[F], F[RestServiceAction]]): HttpService[F] =
    HttpService[F] {
      case request =>
        Monad.flatMap(requestMapper(request))(_.visit(actionVisitor))
    }

}
