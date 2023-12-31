ThisBuild / scalaVersion := "3.3.0"

ThisBuild / organization := "com.clairvoyant.data.scalaxy"

ThisBuild / version := "1.2.0"

ThisBuild / resolvers ++= Seq(
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
  Wart.Overloading,
  Wart.PlatformDefault,
  Wart.Recursion,
  Wart.StringPlusAny,
  Wart.Throw
)

// ----- TOOL VERSIONS ----- //

val dataScalaxyTestUtilVersion = "1.0.0"
val sparkVersion = "3.4.1"

// ----- TOOL DEPENDENCIES ----- //

val dataScalaxyTestUtilDependencies = Seq(
  "com.clairvoyant.data.scalaxy" %% "test-util" % dataScalaxyTestUtilVersion % Test
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
  .map(_ excludeAll ("org.scala-lang.modules", "scala-xml"))
  .map(_.cross(CrossVersion.for3Use2_13))

// ----- MODULE DEPENDENCIES ----- //

val rootDependencies =
  dataScalaxyTestUtilDependencies ++
    sparkDependencies

// ----- SETTINGS ----- //

val rootSettings = Seq(
  Keys.scalacOptions ++= Seq("-Xmax-inlines", "50"),
  libraryDependencies ++= rootDependencies
)

// ----- PROJECTS ----- //

lazy val `transformer` = (project in file("."))
  .settings(rootSettings)
