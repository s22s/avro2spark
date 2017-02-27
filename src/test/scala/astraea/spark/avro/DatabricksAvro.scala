package astraea.spark.avro

import java.util.concurrent.atomic.AtomicInteger

import geotrellis.raster.{ByteArrayTile, Tile}
import org.apache.spark.sql._

object DatabricksAvro {

  private val counter = new AtomicInteger(0)

  def testData: Tile = {
    val i = counter.getAndIncrement()
    ByteArrayTile((1 to 9).map(b ⇒ (b + i).toByte).toArray, 3, 3)
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("avro2spark")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    implicit val mbtEncoder = AvroDerivedSparkEncoder[Tile]

    val gtrdd = sc.makeRDD(Seq.fill(3)(testData))

    val dataSets = Seq(
      gtrdd.toDS
    )

    dataSets.foreach { df ⇒
      println("~" * 60)
      //df.describe()
      //df.explain(true)
      df.show(false)
      df.toJSON.show(false)
    }

    spark.stop()
  }

}
