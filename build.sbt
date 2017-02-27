scalaVersion := "2.11.8"

resolvers += "LocationTech GeoTrellis Releases" at "https://repo.locationtech.org/content/repositories/geotrellis-releases"

resolvers += "LocationTech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots"


def geotrellis(module: String) = "org.locationtech.geotrellis" %% s"geotrellis-$module" % "1.0.0-SNAPSHOT"

def spark(module: String) = "org.apache.spark" %% s"spark-$module" % "2.1.0"

libraryDependencies ++= Seq(
  geotrellis("spark") % "provided",
  geotrellis("spark-testkit") % Test,
  spark("core") % "provided",
  spark("sql") % "provided",
  "com.databricks" %% "spark-avro" % "3.2.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

