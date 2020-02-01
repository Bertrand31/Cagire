package cagire

import scala.util.{Failure, Success, Try}
import cats.effect.{IO, Sync}
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.circe.{jsonEncoder, jsonOf}

object Router {

  var cagire = Cagire.bootstrap()

  implicit val decoder = jsonOf[IO, Array[String]]

  def routes[F[_]: Sync]: HttpRoutes[IO] = {

    HttpRoutes.of[IO] {

      case req @ POST -> Root / "ingest" =>
        req.as[Array[String]].flatMap(paths => {
          Try { cagire.ingestFiles(paths) } match {
            case Failure(err) =>
              InternalServerError(err.getMessage)
            case Success(newCagire) =>
              cagire = newCagire
              Ok("Ingested")
          }
        })

      case GET -> Root / "search-prefix" / prefix =>
        Ok(cagire.searchPrefixAndShow(prefix))

      case GET -> Root / "search" / word =>
        Ok(cagire.searchWordAndFormat(word))
    }
  }
}

