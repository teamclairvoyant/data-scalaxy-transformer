ThisBuild / scalaVersion := "3.3.0"

ThisBuild / organization := "com.clairvoyant.data.scalaxy"

ThisBuild / version := "1.0.0"

ThisBuild / resolvers ++= Seq(
  "DataScalaxyCommon Repo" at "https://maven.pkg.github.com/teamclairvoyant/data-scalaxy-common",
  "DataScalaxyTestUtil Repo" at "https://maven.pkg.github.com/teamclairvoyant/data-scalaxy-test-util"
)

ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  System.getenv("GITHUB_USERNAME"),
  System.getenv("GITHUB_TOKEN")
)

ThisBuild / publishTo := Some(
  "Github Repo" at s"https://maven.pkg.github.com/teamclairvoyant/data-scalaxy-transformer/"
)

// ----- SCALAFIX ----- //

ThisBuild / semanticdbEnabled := true
ThisBuild / scalafixOnCompile := true

// ----- WARTREMOVER ----- //

ThisBuild / wartremoverErrors ++= Warts.allBut(
  Wart.DefaultArguments,
  Wart.Equals,
  Wart.IsInstanceOf,
  Wart.IterableOps,
  Wart.Recursion,
  Wart.Throw
)

// ----- TOOL VERSIONS ----- //

val dataScalaxyCommonVersion = "1.0.0"
val dataScalaxyTestUtilVersion = "1.0.0"

// ----- TOOL DEPENDENCIES ----- //

val dataScalaxyCommonDependencies = Seq(
  "com.clairvoyant.data.scalaxy" %% "common" % dataScalaxyCommonVersion
)

val dataScalaxyTestUtilDependencies = Seq(
  "com.clairvoyant.data.scalaxy" %% "test-util" % dataScalaxyTestUtilVersion % Test
)

// ----- MODULE DEPENDENCIES ----- //

val rootDependencies =
  dataScalaxyCommonDependencies ++
    dataScalaxyTestUtilDependencies

// ----- SETTINGS ----- //

val rootSettings = Seq(
  Keys.scalacOptions ++= Seq("-Xmax-inlines", "50"),
  libraryDependencies ++= rootDependencies
)

// ----- PROJECTS ----- //

lazy val `text-reader` = (project in file("."))
  .settings(rootSettings)
