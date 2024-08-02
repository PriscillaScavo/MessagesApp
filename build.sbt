ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "MessagesApp"
  )

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "2.0.0",
  "org.apache.kafka" % "kafka-clients" % "2.8.0"
)