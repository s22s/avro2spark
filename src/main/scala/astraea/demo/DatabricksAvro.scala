package astraea.demo

import java.time.ZonedDateTime

import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.avro.hack.SchemaConvertersBackdoor
import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteArrayTile, MultibandTile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, GenericRow, Literal, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object DatabricksAvro extends TemporalProjectedExtentCodec {


  val testData = {

    val tpe = TemporalProjectedExtent(Extent(0.0, 0.0, 8.0, 10.0), LatLng, ZonedDateTime.now())

    val tile = MultibandTile(
      ByteArrayTile((1 to 9).map(_ ⇒ 1.toByte).toArray, 3, 3),
      ByteArrayTile((1 to 9).map(_ ⇒ 2.toByte).toArray, 3, 3),
      ByteArrayTile((1 to 9).map(_ ⇒ 3.toByte).toArray, 3, 3)
    )

     (tpe, tile)
  }

  def main(args: Array[String]): Unit = {

    val target = testData

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("avro2spark")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .getOrCreate()

    import spark.implicits._

    @transient
    val sc = spark.sparkContext

    val gtrdd = sc.makeRDD(Seq(target))

    //implicit val tpeEncoder = AvroDerivedEncoder[TemporalProjectedExtent]
    implicit val extentEncoder = AvroDerivedSparkEncoder[Extent]

    val gtdf = gtrdd.map(_._1.extent).toDS

    gtdf.printSchema()

    import org.apache.spark.sql.execution.debug._

    gtdf.show(false)

//    val trans: RDD[Row] = gtrdd.map { p ⇒
//      val encoder = makeRowEncoder(codec)
//      encoder(p)
//    }
//
//    val gtdf = spark.createDataFrame(trans , sqlType.dataType.asInstanceOf[StructType])
//
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
