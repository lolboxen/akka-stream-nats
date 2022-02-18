organization := "com.lolboxen"
name := "akka-stream-nats"
version := "0.1.3"
ThisBuild / versionScheme := Some("semver-spec")

scalaVersion := "2.13.8"

val akkaVer = "2.5.32"

libraryDependencies ++= Seq(
  "io.nats" % "jnats" % "2.13.2",
  "com.typesafe.akka" %% "akka-actor" % akkaVer,
  "com.typesafe.akka" %% "akka-stream" % akkaVer,
  "org.slf4j" % "slf4j-api" % "1.7.35",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVer % Test,
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)

publishTo := Some("GitHub lolboxen Apache Maven Packages" at "https://maven.pkg.github.com/lolboxen/akka-stream-nats")
publishMavenStyle := true
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "lolboxen",
  System.getenv("GITHUB_TOKEN")
)