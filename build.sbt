scalaVersion := "2.11.8"

name := "avro2spark"

organization := "astraea"

version := "0.1.0"

resolvers += "LocationTech GeoTrellis Releases" at "https://repo.locationtech.org/content/repositories/geotrellis-releases"

resolvers += "LocationTech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots"

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

def geotrellis(module: String) = "org.locationtech.geotrellis" %% s"geotrellis-$module" % "1.0.0-SNAPSHOT"

def spark(module: String) = "org.apache.spark" %% s"spark-$module" % "2.0.2"

libraryDependencies ++= Seq(
  geotrellis("spark"),
  spark("core"),
  spark("sql"),
  "com.databricks" %% "spark-avro" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

bintrayOrganization := Some("s22s")

//http://dl.bintray.com/s22s/maven-releases
publishArtifact in (Compile, packageDoc) := false
