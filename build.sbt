organization := "com.github.rubanm"
name := "ignite-scala"
version := "0.0.1"
description := "scala api for distributed closures on apache ignite"

crossScalaVersions := Seq("2.10.5", "2.11.6")
scalaVersion := crossScalaVersions.value.head

val igniteVersion = "1.0.0"
val algebirdVersion = "0.10.1"
val twitterUtilVersion = "6.24.0"

libraryDependencies ++= Seq(
  "org.apache.ignite" % "ignite-core" % igniteVersion,
  "com.twitter" %% "algebird-core" % algebirdVersion,
  "com.twitter" %% "util-logging" % twitterUtilVersion
)

homepage := Some(url(s"https://github.com/softprops/${name.value}/"))

publishArtifact in Test := false
publishMavenStyle := true

pomExtra := (
  <scm>
  <url>git@github.com:rubanm/ignite-scala.git</url>
  <connection>scm:git:git@github.com:rubanm/ignite-scala.git</connection>
  </scm>
  <developers>
  <developer>
  <id>rubanm</id>
  <name>Ruban Monu</name>
    <url>http://github.com/rubanm</url>
  </developer>
  </developers>)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

