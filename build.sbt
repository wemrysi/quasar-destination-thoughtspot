import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-thoughtspot"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-thoughtspot"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-thoughtspot"),
  "scm:git@github.com:precog/quasar-destination-thoughtspot.git"))

val ArgonautVersion = "6.2.3"

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-thoughtspot")
  .settings(
    quasarPluginName := "thoughtspot",
    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
    quasarPluginDestinationFqcn := Some("quasar.physical.ts.TSDestinationModule$"),

    quasarPluginDependencies ++= Seq(
      "io.argonaut"  %% "argonaut"  % ArgonautVersion,
      "com.precog"   %% "fs2-ssh"   % managedVersions.value("precog-fs2-ssh"),
      "org.slf4s"    %% "slf4s-api" % "1.7.25"),

    performMavenCentralSync := false,
    publishAsOSSProject := true)
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
