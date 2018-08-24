package example.gettingstarted.projectionrest

import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import fs2.StreamApp
import io.mewbase.binders.BinderStore
import io.mewbase.rest.RetrieveSingleDocument
import io.mewbase.rest.http4s.MewbaseSupport
import org.http4s.HttpService
import org.http4s.dsl.Http4sDsl
import org.http4s.server.blaze.BlazeBuilder
import scala.concurrent.ExecutionContext.Implicits._

object Main extends StreamApp[IO] with Http4sDsl[IO] with MewbaseSupport {

  val resourceBasename = "example.gettingstarted.projectionrest/configuration.conf"
  val config: Config = ConfigFactory.load(resourceBasename)
  val binderStore: BinderStore = BinderStore.instance(config)

  val service: HttpService[IO] = HttpService[IO] {
    case GET -> Root / "summary" / product / date =>
      RetrieveSingleDocument(binderStore, "sales_summary_projection", s"${product}_$date")
  }

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, StreamApp.ExitCode] =
    BlazeBuilder[IO]
      .bindHttp(8080, "0.0.0.0")
      .mountService(service, "/")
      .serve

}
