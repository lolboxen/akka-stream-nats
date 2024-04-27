ThisBuild / organization := "com.lolboxen"
ThisBuild / version := "0.3.0"
ThisBuild / versionScheme := Some("semver-spec")

ThisBuild / scalaVersion := "3.3.3"

val akkaVer = "2.8.5"

ThisBuild / libraryDependencies ++= Seq(
  "io.nats" % "jnats" % "2.17.6",
  "com.typesafe.akka" %% "akka-actor" % akkaVer,
  "com.typesafe.akka" %% "akka-stream" % akkaVer,
  "org.slf4j" % "slf4j-api" % "2.0.12",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVer % Test,
  "org.scalamock" %% "scalamock" % "6.0.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

ThisBuild / publishTo := Some("GitHub lolboxen Apache Maven Packages" at "https://maven.pkg.github.com/lolboxen/akka-stream-nats")
ThisBuild / publishMavenStyle := true
ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "lolboxen",
  System.getenv("GITHUB_TOKEN")
)