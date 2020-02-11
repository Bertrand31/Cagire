package cagire

import scala.util.{Failure, Success}
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
          cagire.ingestFiles(paths) match {
            case Failure(err) =>
              err.printStackTrace
              InternalServerError(err.getMessage)
            case Success(newCagire) =>
              cagire = newCagire
              Ok("Ingested")
          }
        })

      case GET -> Root / "search" / prefix =>
        Ok(cagire searchPrefixAndFormat prefix)

      case GET -> Root / "search" / word =>
        Ok(cagire searchWordAndFormat word)
    }
  }
}

