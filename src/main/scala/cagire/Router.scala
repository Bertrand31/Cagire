package cagire

import scala.util.Try
import cats.effect.{IO, Sync}
import cats.implicits._
import org.http4s.{HttpRoutes, Response}
import org.http4s.dsl.io._
import org.http4s.circe.{jsonEncoder, jsonOf}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.roaringbitmap.RoaringBitmap

object Router {

  val cagireController = new CagireController

  implicit val decoder = jsonOf[IO, Array[String]]

  object ShowLinesParam extends OptionalQueryParamDecoderMatcher[Boolean]("show-lines")

  private def handleError(err: Throwable): IO[Response[IO]] = {
    err.printStackTrace
    InternalServerError(err.getMessage)
  }

  private def handleTryJson: Try[Json] => IO[Response[IO]] = _.fold(handleError, Ok(_))

  private def applyFormatting(showLines: Option[Boolean])(matchesMap: Map[Int, RoaringBitmap]) =
    if (showLines getOrElse false)
      handleTryJson(cagireController.formatExtended(matchesMap))
    else
      Ok(cagireController.formatBasic(matchesMap))

  def routes[F[_]: Sync]: HttpRoutes[IO] = {

    HttpRoutes.of[IO] {

      case req @ POST -> Root / "ingest" =>
        req.as[Array[String]] >>= (paths =>
          handleTryJson(
            cagireController.ingestFiles(paths).map(_ => "Ingested".asJson)
          )
        )

      case GET -> Root / "search-and" / words :? ShowLinesParam(showLines) =>
        applyFormatting(showLines) {
          cagireController.searchWordsWithAnd(words)
        }

      case GET -> Root / "search-or" / words :? ShowLinesParam(showLines) =>
        applyFormatting(showLines) {
          cagireController.searchWordsWithOr(words)
        }
    }
  }
}

