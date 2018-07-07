package io.mewbase.rest.http4s

import java.util.concurrent.{CompletableFuture, TimeUnit}

import BsonEntityCodec._
import cats.effect.IO
import com.typesafe.config.Config
import io.mewbase.binders.BinderStore
import io.mewbase.bson.BsonObject
import io.mewbase.cqrs.{CommandManager, QueryManager}
import io.mewbase.rest.{RestServiceAction, RestServiceAdaptor}
import org.http4s.{HttpService, Request, Response}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Server
import org.http4s.server.blaze.BlazeBuilder

import scala.collection.mutable.ListBuffer

class Http4sRestServiceAdapter(config: Config) extends RestServiceAdaptor with Http4sDsl[IO] {

  val port = config.getInt("mewbase.api.rest.http4s.port")

  val server =
    BlazeBuilder[IO]
      .bindLocal(port)

  var runningServer: Option[Server[IO]] = None

  val actionVisitor = new Http4sRestServiceActionVisitor[IO]

  val endpoints = new ListBuffer[PartialFunction[Request[IO], IO[Response[IO]]]]

  private[this] def prefixWithoutSlash(prefix: String): String =
    if (prefix.startsWith("/"))
      prefix.tail
    else
      prefix

  /**
    * This will setup a set of routes (paths) to every document in every binder in the form
    *
    * Verb : GET
    * URI : http://Server:Port/binders/{binderName}/{binderId}
    *
    * In the case where both binderName and binderId is missing ...
    *
    * Hence -> http://http://Server:Port/binders
    *
    * The call will list all of the binder names defined in the current context.
    *
    * In the case where a valid binderName is given it will list all of the documentIds for that binder.
    *
    * In the case where both the binderId and the documentIds is given
    *
    * This method is functionally equivalent to calling exposeGetDocument(binderStore, "/binders" );
    *
    * @param binderStore
    * @return
    */
  override def exposeGetDocument(binderStore: BinderStore): RestServiceAdaptor = {
    exposeGetDocument(binderStore, "binders")
  }

  /**
    * This method is functionally the similar to @exposeGetDocument(BinderStore binderStore) however it can be
    * used to insert a URI path prefix that subsumes the 'binders' path hence ...
    *
    * if uriPathPrefix == "/recipes" then URI for accessing the binders would be contextualised to
    *
    * Verb : GET
    * URI : http://Server:Port/recipes/{binderName}/{binderId}
    *
    *
    * If the uriPathPrefix is the empty string "" the binder name will become the first path element.
    *
    * @param binderStore
    * @param uriPathPrefix
    * @return
    */
  override def exposeGetDocument(binderStore: BinderStore, uriPathPrefix: String): RestServiceAdaptor = {
    val prefix = prefixWithoutSlash(uriPathPrefix)
    endpoints += {
      case GET -> Root / `prefix` =>
        actionVisitor.visit(RestServiceAction.listBinders(binderStore))

      case GET -> Root / `prefix` / binderName =>
        actionVisitor.visit(RestServiceAction.listDocumentIds(binderStore, binderName))

      case GET -> Root / `prefix` / binderName / documentId =>
        actionVisitor.visit(RestServiceAction.retrieveSingleDocument(binderStore, binderName, documentId))
    }
    this
  }

  /**
    * This method is exposes only the named Binder in the binder store and all of its contained
    * documents. It places a single limit on the scope of the binders that can be accessed.
    *
    * Path prefix behaviour is the same as for above.
    *
    * @param binderStore
    * @param uriPathPrefix
    * @return
    */
  override def exposeGetDocument(binderStore: BinderStore, binderName: String, uriPathPrefix: String): RestServiceAdaptor = {
    val prefix = prefixWithoutSlash(uriPathPrefix)
    endpoints += {
      case GET -> Root / `prefix` / `binderName` =>
        actionVisitor.visit(RestServiceAction.listDocumentIds(binderStore, binderName))
      case GET -> root / `prefix` / `binderName` / documentId =>
        actionVisitor.visit(RestServiceAction.retrieveSingleDocument(binderStore, binderName, documentId))
    }
    this
  }

  /**
    * Todo
    *
    * @param commandManager
    * @param commandName
    * @return
    */
  override def exposeCommand(commandManager: CommandManager, commandName: String): RestServiceAdaptor = {
    endpoints += {
      case req@POST -> Root / `commandName` =>
        req.as[BsonObject].flatMap { context =>
          actionVisitor.visit(RestServiceAction.executeCommand(commandManager, commandName, context))
        }
    }
    this
  }

  override def exposeCommand(commandManager: CommandManager, commandName: String, uriPathPrefix: String): RestServiceAdaptor = {
    val prefix = prefixWithoutSlash(uriPathPrefix)
    endpoints += {
      case req@POST -> Root / `prefix` / `commandName` =>
        req.as[BsonObject].flatMap { context =>
          actionVisitor.visit(RestServiceAction.executeCommand(commandManager, commandName, context))
        }
    }
    this
  }

  /**
    * Todo
    *
    * @param queryManager
    * @param queryName
    * @return
    */
  override def exposeQuery(queryManager: QueryManager, queryName: String): RestServiceAdaptor = {
    endpoints += {
      case GET -> Root / `queryName` =>
        actionVisitor.visit(RestServiceAction.runQuery(queryManager, queryName, new BsonObject()))
    }
    this
  }

  override def exposeQuery(queryManager: QueryManager, queryName: String, uriPathPrefix: String): RestServiceAdaptor = {
    val prefix = prefixWithoutSlash(uriPathPrefix)
    endpoints += {
      case GET -> Root / `prefix` =>
        actionVisitor.visit(RestServiceAction.runQuery(queryManager, queryName, new BsonObject()))
    }
    this
  }

  /**
    * Start this REST adapter asynchronously.
    * Multiple rest adapters may be started on the same host and
    * bound to different endpoints (ipaddr:port) combinations.
    *
    * @return
    */
  override def start(): CompletableFuture[Void] =
    CompletableFuture.runAsync(() => {
      runningServer = {
        val compiledEndpoints =
          endpoints
            .foldLeft(PartialFunction.empty[Request[IO], IO[Response[IO]]]) {
              (next, current) => current.orElse(next)
            }

        val service = HttpService[IO](compiledEndpoints)

        Some(server
          .mountService(service, "/")
          .start
          .unsafeRunSync())
      }
    })

  override def getServerPort: Int = runningServer.get.address.getPort

  /**
    * Stop this REST adapter asynchronously.
    *
    * @return
    */
  override def stop(): CompletableFuture[Void] =
    CompletableFuture.runAsync(() => runningServer.foreach(_.shutdownNow()))

  override def close(): Unit =
    stop.get(5, TimeUnit.SECONDS)

}
