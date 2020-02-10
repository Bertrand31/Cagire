name := "Cagire"

version := "1"

maintainer := "bertrandjun@gmail.com"

scalaVersion := "2.12.10"

val CirceVersion = "0.12.3"
val Http4sVersion = "0.20.17"
val Specs2Version = "4.1.0"

libraryDependencies ++= Seq(
  // Web server
  "org.http4s"    %% "http4s-blaze-server" % Http4sVersion,
  "org.http4s"    %% "http4s-blaze-client" % Http4sVersion,
  "org.http4s"    %% "http4s-circe" % Http4sVersion,
  "org.http4s"    %% "http4s-dsl" % Http4sVersion,
  // JSON encoding and decoding
  "io.circe"      %% "circe-generic" % CirceVersion,
  // Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  // Tests
  "org.scalatest" %% "scalatest" % "3.1.0",
  // Misc
  "org.roaringbitmap" % "RoaringBitmap" % "0.8.9",
  "org.typelevel" %% "cats-core" % "2.1.0",
  // JSON
  "io.circe" %% "circe-core" % CirceVersion,
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-parser" % CirceVersion,
)

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.0")

scalacOptions ++= Seq(
  "-Ypartial-unification",
  "-deprecation", // Warn about deprecated features
  "-encoding", "UTF-8", // Specify character encoding used by source files
  "-feature", // Emit warning and location for usages of features that should be imported explicitly
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:higherKinds", // Allow higher-kinded types
  "-unchecked", // Enable additional warnings where generated code depends on assumptions
  "-Xlint:_", // Enable all available style warnings
  "-Ywarn-macros:after", // Only inspect expanded trees when generating unused symbol warnings
  "-Ywarn-unused:_", // Enables all unused warnings
  "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
)

scalacOptions in Test --= Seq(
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
