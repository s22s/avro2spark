scalaVersion := "2.11.8"

name := "avro2spark"

organization := "astraea"

version := "0.1.5-SNAPSHOT"

resolvers ++= Seq(
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  "boundlessgeo" at "https://repo.boundlessgeo.com/main",
  "osgeo" at "http://download.osgeo.org/webdav/geotools",
  "conjars.org" at "http://conjars.org/repo"
)


licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

def geotrellis(module: String) = "org.locationtech.geotrellis" %% s"geotrellis-$module" % "1.0.0-SNAPSHOT"

def spark(module: String) = "org.apache.spark" %% s"spark-$module" % "2.1.0"

libraryDependencies ++= Seq(
  geotrellis("spark") % "provided",
  geotrellis("spark-testkit") % Test,
  spark("core") % "provided",
  spark("sql") % "provided",
  //"org.locationtech.geomesa" %% "geomesa-spark-sql" % "1.3.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

bintrayOrganization := Some("s22s")

bintrayReleaseOnPublish in ThisBuild := false

//http://dl.bintray.com/s22s/maven-releases
publishArtifact in (Compile, packageDoc) := false
