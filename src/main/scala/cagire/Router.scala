package cagire

import scala.util.Try
import cats.effect.{IO, Sync}
import cats.implicits._
import org.http4s.{HttpRoutes, Response}
import org.http4s.dsl.io._
import org.http4s.circe.{jsonEncoder, jsonOf}
import io.circe.Json
import io.circe.syntax.EncoderOps

object Router {

  val cagireController = new CagireController

  implicit val decoder = jsonOf[IO, Array[String]]

  object ShowLinesParam extends OptionalQueryParamDecoderMatcher[Boolean]("show-lines")

  private def handleError(err: Throwable): IO[Response[IO]] = {
    err.printStackTrace
    InternalServerError(err.getMessage)
  }

  private def handleTryJson: Try[Json] => IO[Response[IO]] = _.fold(handleError, Ok(_))

  def routes[F[_]: Sync]: HttpRoutes[IO] = {

    HttpRoutes.of[IO] {

      case req @ POST -> Root / "ingest" =>
        req.as[Array[String]] >>= (paths =>
          {
            cagireController.ingestFiles(paths)
            Ok("Ingested")
          }
        )

      case GET -> Root / "search-prefix" / prefix :? ShowLinesParam(showLines) =>
        if (showLines getOrElse false)
          handleTryJson(cagireController.searchPrefixAndGetLines(prefix))
        else
          Ok(cagireController.searchPrefixAndGetMatches(prefix))

      case GET -> Root / "search" / word :? ShowLinesParam(showLines) =>
        if (showLines getOrElse false)
          handleTryJson(cagireController.searchWordAndGetLines(word))
        else
          Ok(cagireController.searchWordAndGetMatches(word))
    }
  }
}
