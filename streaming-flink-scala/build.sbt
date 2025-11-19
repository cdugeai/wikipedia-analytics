val scala3Version = "3.6.2"
val flinkVersion = "2.1.1" // Matching Flink cluster's version

lazy val root = project
  .in(file("."))
  .settings(
    name := "streaming-flink-scala",
    version := "0.1.0",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,

  )

libraryDependencies ++= Seq(
  // [ SCALA API ] Flink deps not compatible with Scala 3
  //"org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  //"org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  //"org.apache.flink" %% "flink-clients" % flinkVersion % "provided",

  // [ JAVA API ] Use Java APIs (no %%)
  "org.apache.flink" % "flink-core" % flinkVersion % "provided",
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
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