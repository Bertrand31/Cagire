name := "Cagire"

version := "1"

scalaVersion := "3.1.0"

val CirceVersion = "0.14.1"
val Http4sVersion = "0.22.0"

libraryDependencies ++= Seq(
  // Web server
  "org.http4s"    %% "http4s-blaze-server" % Http4sVersion,
  "org.http4s"    %% "http4s-blaze-client" % Http4sVersion,
  "org.http4s"    %% "http4s-circe" % Http4sVersion,
  "org.http4s"    %% "http4s-dsl" % Http4sVersion,
  // JSON encoding and decoding
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-core" % CirceVersion,
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-parser" % CirceVersion,
  // Tests
  "org.scalatest" %% "scalatest" % "3.2.12",
  // Misc
  "org.roaringbitmap" % "RoaringBitmap" % "0.9.27",
  "org.typelevel" %% "cats-core" % "2.7.0",
)

scalacOptions ++= Seq(
  "-deprecation", // Warn about deprecated features
  "-encoding", "UTF-8", // Specify character encoding used by source files
  "-feature", // Emit warning and location for usages of features that should be imported explicitly
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:higherKinds", // Allow higher-kinded types
  "-unchecked", // Enable additional warnings where generated code depends on assumptions
)

Test / scalacOptions --= Seq(
  "-Xlint:_",
  "-Ywarn-unused-import",
)

javaOptions ++= Seq(
  "-XX:+CMSClassUnloadingEnabled", // Enable class unloading under the CMS GC
  "-Xms2g",
  "-Xmx12g",
  // "-XX:+UseParNewGC",
)

enablePlugins(JavaServerAppPackaging)
