package astraea.demo

import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicInteger

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteArrayTile, MultibandTile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.beans.BeanProperty

object DatabricksAvro {

  private val counter = new AtomicInteger(0)

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
      gtrdd.map(_._2).toDS
      //gtrdd.map(_._2).toDS.select(explode('bands))
    )

    dataFrames.foreach { df ⇒
      println("~" * 60)
      df.describe()
      df.explain(true)
      df.show(false)
      df.toJSON.show(false)
    }

    spark.stop()
  }



  case class Foo(stuff: Seq[Int], name: String, other: Double)

  class Foo2(
    @BeanProperty var stuff: java.util.List[java.lang.Integer],
    @BeanProperty var name: String,
    @BeanProperty var other: Double
  )

  def pokeEncoders(): Unit = {

    println("#" * 60)

    val enc = Encoders.product[Foo].asInstanceOf[ExpressionEncoder[Foo]]
    enc.serializer.foreach(e ⇒ println(e.treeString(true)))
    println(enc.deserializer.treeString(true))

    println("#" * 60)
    val enc2 = Encoders.bean[Foo2](classOf[Foo2]).asInstanceOf[ExpressionEncoder[Foo2]]
    enc2.serializer.foreach(e ⇒ println(e.treeString(true)))
    println(enc2.deserializer.treeString(true))

    println("#" * 60)

    val enc3 = ExpressionEncoder[(Int, String)]
    enc3.serializer.foreach(e ⇒ println(e.treeString(true)))
    println(enc3.deserializer.treeString(true))

    println("#" * 60)

  }
}
