organization := "me.rubanm"

name := "ignite-scala"

version := "0.0.1"

description := "scala api for distributed closures on apache ignite"

scalaVersion := "2.10.4"

val igniteVersion = "1.0.0"

val algebirdVersion = "0.10.1"

val twitterUtilVersion = "6.24.0"

libraryDependencies ++= Seq(
  "org.apache.ignite" % "ignite-core" % igniteVersion,
  "com.twitter" % "algebird-core_2.10" % algebirdVersion,
  "com.twitter" % "util-logging_2.10" % twitterUtilVersion
)
