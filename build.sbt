ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val sparkVersion = "3.2.0"
val json4sVersion = "3.6.6"

lazy val root = (project in file("."))
  .settings(
    name := "pollution-data-processing-service"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Compile,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Compile,
  "com.typesafe" % "config" % "1.4.2",
  "org.json4s" %% "json4s-native" % json4sVersion
)
