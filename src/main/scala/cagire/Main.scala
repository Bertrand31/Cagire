package cagire

import scala.concurrent.duration.DurationInt
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import org.http4s.implicits._
import org.http4s.blaze.server.BlazeServerBuilder

object Server {

  def stream[F[_]]: Stream[IO, Nothing] = {

    val httpApp = (
      Router.routes[IO]
    ).orNotFound

    BlazeServerBuilder[IO]
      .bindHttp(8080, "0.0.0.0")
      .withIdleTimeout(10.minutes)
      .withHttpApp(httpApp)
      .serve
  }.drain
}

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    Server
      .stream[IO]
      .compile
      .drain
      .as(ExitCode.Success)
}
