import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-thoughtspot"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-thoughtspot"),
  "scm:git@github.com:slamdata/quasar-destination-thoughtspot.git"))

val quasarVersion = IO.read(file("./quasar-version")).trim
val fs2SshVersion = IO.read(file("./fs2-ssh-version")).trim

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
    quasarPluginName := "s3",
    quasarPluginQuasarVersion := quasarVersion,
    // quasarPluginDestinationFqcn := Some(""),

    quasarPluginDependencies += "com.slamdata" %% "fs2-ssh" % fs2SshVersion,

    performMavenCentralSync := false,
    publishAsOSSProject := true)
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
