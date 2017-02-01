package astraea.demo

import geotrellis.proj4.LatLng
import geotrellis.raster.{ArrayMultibandTile, ByteArrayTile, ByteConstantTile, IntConstantTile, MultibandTile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import com.databricks.spark.avro.SchemaConverters
import java.time.ZonedDateTime

import com.databricks.spark.avro.hack.SchemaConvertersBackdoor
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DatabricksAvro extends TemporalProjectedExtentCodec {

  def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]

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

//    val codec = codecOf(target)
//
//    //println(s"codec.schema.toString(true): ${codec.schema.toString(true)}")
//
//    //println("*" * 50)
//
//    val sqlType = SchemaConverters.toSqlType(codec.schema)
//
//    def makeRowEncoder[T](codec: AvroRecordCodec[T]) = {
//      val sqlType = SchemaConverters.toSqlType(codec.schema)
//      //println(s"sqlType.dataType.prettyJson: ${sqlType.dataType.prettyJson}")
//
//      val structType = sqlType.dataType match {
//        case st: StructType ⇒ st
//        case _ ⇒ throw new IllegalArgumentException(s"${sqlType.dataType} not a struct")
//      }
//
//      val avroEncoder: (T) ⇒ GenericRecord = codec.encode
//
//      val structTranslator = SchemaConvertersBackdoor.createConverterToSQL(codec.schema, structType)
//
//      val cast = (a: AnyRef) ⇒ a.asInstanceOf[Row]
//
//      avroEncoder andThen structTranslator andThen cast
//    }
//
//    // val encoder = makeRowEncoder(codec)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("avro2spark")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .getOrCreate()

    import spark.implicits._

    @transient
    val sc = spark.sparkContext

    val gtrdd = sc.makeRDD(Seq(target))

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
