scalaVersion := "2.13.8"
name := "spark-streaming-sql"
organization := "helium"
version := "1.0"

val sparkVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "org.codehaus.janino" % "janino" % "3.0.16",
  "io.delta" %% "delta-core" % "2.4.0",
  "com.amazonaws" % "aws-java-sdk" % "1.11.469",
  "org.scalatest" %% "scalatest" % "3.2.16" % "test"
)
