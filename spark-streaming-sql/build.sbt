scalaVersion := "2.13.8"
name := "spark-streaming-sql"
organization := "helium"
version := "1.0.2"

val sparkVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1" % "provided",
  "org.codehaus.janino" % "janino" % "3.0.16",
  "io.delta" %% "delta-core" % "2.4.0",
  "com.amazonaws" % "aws-java-sdk" % "1.11.469" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.16" % "test"
)

// assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion.value}_${version.value}.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services", xg @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
