scalaVersion := "2.11.8"

resolvers += "LocationTech GeoTrellis Releases" at "https://repo.locationtech.org/content/repositories/geotrellis-releases"

resolvers += "LocationTech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots"


def geotrellis(module: String) = "org.locationtech.geotrellis" %% s"geotrellis-$module" % "1.0.0-SNAPSHOT"

def spark(module: String) = "org.apache.spark" %% s"spark-$module" % "2.0.2"

libraryDependencies ++= Seq(
  geotrellis("spark"),
  spark("core"),
  spark("sql"),
  "com.databricks" %% "spark-avro" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

