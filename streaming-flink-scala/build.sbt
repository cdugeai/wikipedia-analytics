val scala3Version = "3.6.2"
val flinkVersion = "1.20.3" // Matching Flink cluster's version
val scala2Version = "2.12.18"

lazy val root = project
  .in(file("."))
  .settings(
    name := "streaming-flink-scala",
    version := "0.2.0",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,

  )

libraryDependencies ++= Seq(
  // [ SCALA API ] Flink deps not compatible with Scala 3
  //"org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  //"org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  // [ JAVA API ] Flink deps not compatible with Scala 3
  //"org.apache.flink" % "flink-clients" % flinkVersion % "provided",


  "org.flinkextended" %% "flink-scala-api-1" % "1.2.6",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  //"org.flinkextended" %% "flink-scala-api" % "1.20.0_1.2.0",
  //"org.apache.flink" % "flink-clients" % "1.20.3"


  // [ JAVA API ] Use Java APIs (no %%)
  //"org.apache.flink" % "flink-core" % flinkVersion % "provided",
  //"org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  //"org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  //"org.apache.flink" % "flink-connector-kafka" % flinkVersion
)

// Set main class
// Compile / mainClass := Some("WordCountJob")

// For assembly plugin
// assembly / mainClass := Some("WordCountJob")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}