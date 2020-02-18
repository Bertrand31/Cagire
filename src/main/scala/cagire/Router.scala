package cagire

import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import cats.effect.{IO, Sync}
import cats.implicits._
import org.http4s.{HttpRoutes, Response}
import org.http4s.dsl.io._
import org.http4s.circe.{jsonEncoder, jsonOf}
import io.circe.Json
import io.circe.syntax.EncoderOps

object Router {

  var cagire = Cagire.bootstrap()

  implicit val decoder = jsonOf[IO, Array[String]]

  private def handleError(err: Throwable): IO[Response[IO]] = {
    err.printStackTrace
    InternalServerError(err.getMessage)
  }

  private def handleTryJson: Try[Json] => IO[Response[IO]] = _.fold(handleError, Ok(_))

  def routes[F[_]: Sync]: HttpRoutes[IO] = {

    HttpRoutes.of[IO] {

      case req @ POST -> Root / "ingest" =>
        req.as[Array[String]] >>= (paths =>
          handleTryJson(
            cagire.ingestFiles(paths)
              .tap(_ foreach { this.cagire = _ })
              .map(_ => "Ingested".asJson)
          )
        )

      case GET -> Root / "search-prefix" / prefix =>
        handleTryJson(cagire searchPrefixAndFormat prefix)

      case GET -> Root / "search" / word =>
        handleTryJson(cagire searchWordAndFormat word)
    }
  }
}

