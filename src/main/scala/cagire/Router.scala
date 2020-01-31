package cagire

import cats.effect.{IO, Sync}
import org.http4s.HttpRoutes
import org.http4s.dsl.io.{GET, http4sOkSyntax, Ok, POST, Root, /, ->}
import io.circe.syntax._
import org.http4s.circe._

object Router {

  var cagire = Cagire.bootstrap()

  def routes[F[_]: Sync]: HttpRoutes[IO] = {

    HttpRoutes.of[IO] {

      case POST -> Root / "ingest" / path =>
        cagire = cagire.ingestFile(path)
        Ok("Ingested")

      case GET -> Root / "search-prefix" / prefix =>
        Ok(cagire.searchPrefixAndShow(prefix).asJson)

      case GET -> Root / "search" / word =>
        Ok(cagire.searchWordAndFormat(word).asJson)
    }
  }
}

