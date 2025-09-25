ThisBuild / organization     := "io.github.platob"
ThisBuild / scalaVersion     := Dependencies.V.scala213
ThisBuild / crossScalaVersions := Seq(Dependencies.V.scala212, Dependencies.V.scala213)

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding", "utf-8",
  "-Xlint:_",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

ThisBuild / Test / javaOptions ++= Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-exports", "java.base/sun.net.www.http=ALL-UNNAMED",
  "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports", "java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-exports", "jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
  "--add-opens", "java.base/java.io=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang=ALL-UNNAMED",
  "--add-opens", "java.base/java.math=ALL-UNNAMED",
  "--add-opens", "java.base/java.util=ALL-UNNAMED",
  "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens", "java.base/java.net=ALL-UNNAMED",
  "--add-opens", "java.base/java.text=ALL-UNNAMED",
  "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens", "java.base/java.nio=ALL-UNNAMED",
  "--add-opens", "java.base/sun.net.www.http=ALL-UNNAMED",
  "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens", "java.base/java.time=ALL-UNNAMED",
  "--add-opens", "java.base/java.util.regex=ALL-UNNAMED",
  "--add-opens", "java.base/java.util=ALL-UNNAMED",
  "--add-opens", "java.base/jdk.internal=ALL-UNNAMED",
  "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens", "java.base/jdk.internal.reflect=ALL-UNNAMED",
  "--add-opens", "java.sql/java.sql=ALL-UNNAMED",
  "--add-opens", "java.base/jdk.internal.util=ALL-UNNAMED",
  "--add-opens", "java.base/jdk.internal.util.random=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens", "jdk.management/com.sun.management.internal=ALL-UNNAMED",
  "--add-opens", "java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
)
ThisBuild / Test / parallelExecution := false

lazy val commonSettings = Seq(
  libraryDependencies ++= Dependencies.test
)

lazy val root = (project in file("."))
  .aggregate(core, spark)
  .settings(
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "arrow4s-core",
    libraryDependencies ++= Dependencies.arrowCore ++ Seq(
      // Scala reflect
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    )
  )

lazy val io = (project in file("io"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "arrow4s-io",
    libraryDependencies ++= Dependencies.arrowIO
  )

lazy val spark = (project in file("spark"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "arrow4s-spark",
    libraryDependencies ++= Dependencies.sparkProvided
  )

// ----------- Publishing (sbt-ci-release) -----------
ThisBuild / homepage := Some(url("https://github.com/Platob/arrow4s"))
ThisBuild / licenses := Seq("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / scmInfo  := Some(
  ScmInfo(
    url("https://github.com/Platob/arrow4s"),
    "scm:git:git@github.com:Platob/arrow4s.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "Platob",
    name  = "Nicolas",
    email = "nfillot.pro@gmail.com",
    url   = url("https://github.com/Platob")
  )
)
