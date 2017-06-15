
scalacOptions ++= Seq("-Yrangepos", "-unchecked", "-deprecation")

version := "0.2-SNAPSHOT"

name := "centrifuge"

organization := "io.univalence"

scalaVersion := "2.11.7"

val sparkV = "2.1.1"


resolvers ++= Seq(
  "sonatype-oss" at "http://oss.sonatype.org/content/repositories/snapshots",
  "OSS" at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Conjars" at "http://conjars.org/repo",
  "Clojars" at "http://clojars.org/repo",
  "m2-repo-github" at "https://github.com/ahoy-jon/m2-repo/raw/master"
)


libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.2.5",
  "org.scalaz" %% "scalaz-core" % "7.1.4",
  "org.typelevel" %% "shapeless-scalaz" % "0.4",
  "io.univalence" %% "excelsius" % "0.1-SNAPSHOT",
  "org.apache.spark" %% "spark-core" % sparkV,
  "org.apache.spark" %% "spark-sql" % sparkV,
  "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
)

//2.1.0-SNAPSHOT
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

publishTo := Some(Resolver.file("file",  new File( "/Users/jon/Project/m2-repo")))