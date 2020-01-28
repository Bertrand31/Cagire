name := "Cagire"

version := "1"

scalaVersion := "2.13.1"

val CirceVersion = "0.12.3"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0",
  // JSON
  "io.circe" %% "circe-core" % CirceVersion,
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-parser" % CirceVersion,
)

scalacOptions ++= Seq(
  "-Ymacro-annotations", // Needed by Circe
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
