name := "FileLoaderMicroservice"

version := "1.0"

scalaVersion := "2.12.2"

val akkaVersion = "2.4.12"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.13",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-core" % "10.0.5",
  "com.typesafe.akka" %% "akka-http" % "10.0.5",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.5",

  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.5",
  "io.spray"          %%  "spray-json"    % "1.2.5",

  "com.typesafe.akka" %% "akka-http-jackson" % "10.0.5",
  "com.typesafe.akka" %% "akka-http-xml" % "10.0.5",
  "net.liftweb" %% "lift-json" % "3.1.0-M2"

)