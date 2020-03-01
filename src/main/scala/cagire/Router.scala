package cagire

import scala.util.Try
import cats.effect.{IO, Sync}
import cats.implicits._
import org.http4s.{HttpRoutes, Response}
import org.http4s.dsl.io._
import org.http4s.circe.{jsonEncoder, jsonOf}
import io.circe.Json
import io.circe.syntax.EncoderOps
import akka.actor.ActorSystem
import akka.util.Timeout

object Router {

  import CagireHandler._
  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit val requestTimeout: Timeout = {
    val d = Duration("30s")
    FiniteDuration(d.length, d.unit)
  }
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher

  def cagireHandler = system.actorOf(CagireHandler.props)

  implicit val decoder = jsonOf[IO, List[String]]

  object ShowLinesParam extends OptionalQueryParamDecoderMatcher[Boolean]("show-lines")

  private def handleError(err: Throwable): IO[Response[IO]] = {
    err.printStackTrace
    InternalServerError(err.getMessage)
  }

  private def handleTryJson: Try[Json] => IO[Response[IO]] = _.fold(handleError, Ok(_))

  def routes[F[_]: Sync]: HttpRoutes[IO] = {

    HttpRoutes.of[IO] {

      case req @ POST -> Root / "ingest" =>
        req.as[List[String]] >>= (paths => {
          cagireHandler.ask(IngestPaths(paths))
          Ok("Ingested".asJson)
        })

      // case GET -> Root / "search-prefix" / prefix :? ShowLinesParam(showLines) =>
        // if (showLines getOrElse false)
          // handleTryJson(cagireController.searchPrefixAndGetLines(prefix))
        // else
          // Ok(cagireController.searchPrefixAndGetMatches(prefix))

      // case GET -> Root / "search" / word :? ShowLinesParam(showLines) =>
        // if (showLines getOrElse false)
          // handleTryJson(cagireController.searchWordAndGetLines(word))
        // else
          // Ok(cagireController.searchWordAndGetMatches(word))
    }
  }
}
