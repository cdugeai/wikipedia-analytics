name := "streaming-flink-scala"

version := "0.1.0"

scalaVersion := "2.12.18" // must match the scala library installed onto the Flink image

val flinkVersion = "1.20.3"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "upickle" % "4.1.0",
  "com.softwaremill.sttp.client4" %% "core" % "4.0.9",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.20.1",
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.slf4j" % "slf4j-api" % "2.0.17" % "provided",
  "org.apache.flink" % "flink-connector-kafka" % "1.17.0"

)

// Ensure the scala-library version you compile with is deterministic
dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

// Fork run/tests to avoid sbt classloader issues during local development
Compile / fork := true
Test / fork := true

Compile / mainClass := Some("com.example.KafkaReader")


// Assembly configuration
assembly / mainClass := Some("com.example.KafkaReader")

// Exclude Scala from deps when building fat JAR
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => sbtassembly.MergeStrategy.discard
  case "reference.conf" => sbtassembly.MergeStrategy.concat
  case _ => sbtassembly.MergeStrategy.first
}

// Make 'provided' dependencies available for 'sbt run'
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

Compile / runMain := Defaults.runMainTask(
  Compile / fullClasspath,
  Compile / run / runner
).evaluated