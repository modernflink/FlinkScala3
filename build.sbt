Global / onChangedBuildSource := ReloadOnSourceChanges

// give the user a nice default project!
ThisBuild / organization := "com.example"
ThisBuild / scalaVersion := "3.3.0"

val flinkVersion = "1.17.0"
val flinkDependencies = Seq(
  ("org.flinkextended" %% "flink-scala-api" % s"1.17.1_1.0.0")
    .excludeAll(
      ExclusionRule(organization = "org.apache.flink")
    ),
  "org.apache.flink" % "flink-runtime-web" % flinkVersion % Provided,
  "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
  "org.apache.flink" % "flink-test-utils" % flinkVersion % Test,
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % Test classifier ("tests"),
  "org.scalatest" %% "scalatest" % "3.2.13" % Test
)
lazy val root = (project in file(".")).settings(
  name := "my-flink-scala-proj",
  assembly / mainClass := Some("com.example.wordCount"),
  libraryDependencies ++= flinkDependencies, //Seq(
//    "org.flinkextended" %% "flink-scala-api" % "1.17.1_1.0.0",
////    "org.apache.flink" % "flink-connector-kafka" % "1.17.0",
//    "org.apache.flink" % "flink-clients" % flinkVersion
  //),
//  assemblyMergeStrategy := {
//    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
//    case PathList("org", "apache", "flink", _*)         => MergeStrategy.first
//    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
//    case "META-INF/io.netty.versions.properties" => MergeStrategy.discard
//    case "scala-collection-compat.properties" => MergeStrategy.first
//    case "application.conf"                            => MergeStrategy.concat
//    case "unwanted.txt"                                => MergeStrategy.discard
//    case x =>
//      val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
//      oldStrategy(x)
//  }
)

//assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false)

ThisBuild / assemblyMergeStrategy := {
//  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "META-INF/io.netty.versions.properties"                => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// make run command include the provided dependencies
Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { f =>
        Set(
          "scala-asm-9.3.0-scala-1.jar",
          "interface-1.0.4.jar",
          "scala-compiler-2.13.6.jar"
        ).contains(
          f.data.getName
        )
      }
    }
