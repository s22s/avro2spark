package astraea.demo

import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicInteger

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteArrayTile, MultibandTile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DatabricksAvro extends TemporalProjectedExtentCodec {

  val counter = new AtomicInteger(0)

  def testData: (TemporalProjectedExtent, MultibandTile) = {

    val i = counter.getAndIncrement()

    val tpe = TemporalProjectedExtent(Extent(1.0 + i, 2.0 + i, 3.0 + i, 4.0 + i), LatLng, ZonedDateTime.now())

    val tile = MultibandTile(
      ByteArrayTile((1 to 9).map(_ ⇒ (1+i).toByte).toArray, 3, 3),
      ByteArrayTile((1 to 9).map(_ ⇒ (2+i).toByte).toArray, 3, 3),
      ByteArrayTile((1 to 9).map(_ ⇒ (3+i).toByte).toArray, 3, 3)
    )

     (tpe, tile)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("avro2spark")
      .getOrCreate()

    import spark.implicits._

    @transient
    val sc = spark.sparkContext


    implicit val tpeEncoder = AvroDerivedSparkEncoder[TemporalProjectedExtent]
    implicit val mbtEncoder = AvroDerivedSparkEncoder[MultibandTile]
    implicit val kvEncoder = AvroDerivedSparkEncoder[(TemporalProjectedExtent, MultibandTile)]//Encoders.tuple(tpeEncoder, mbtEncoder)

    val gtrdd = sc.makeRDD(Seq.fill(3)(testData))

    val dataFrames = Seq(
      gtrdd.toDS,
      gtrdd.map(_._1).toDS,
      gtrdd.map(_._2).toDS,
      gtrdd.map(_._2).toDS.select(explode('bands))
    )

    dataFrames.foreach { df ⇒
      println("~" * 60)
      df.describe()
      df.explain()
      df.show(false)
      df.toJSON.show(false)
    }


//    val tsum = udf((row: Array[Byte]) ⇒ row.sum)
//    //val tilef = udf((t: ArrayMultibandTile) ⇒ t)
//
//    val sel = gtdf.select(explode($"_2.bands.member0.cells").as("cells")).select(tsum($"cells"))
//      //.select(coalesce($"member3", $"member1", $"member0"))
//    sel.printSchema()
//    sel.show(false)


//    val del = gtdf.select(tilef($"_2"))
//    del.printSchema()
//    del.show(false)

    spark.stop()
  }
}
