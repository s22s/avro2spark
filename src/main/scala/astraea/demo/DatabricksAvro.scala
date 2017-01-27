package astraea.demo

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteArrayTile, ByteConstantTile, IntConstantTile, MultibandTile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import com.databricks.spark.avro.SchemaConverters
import java.time.ZonedDateTime

import com.databricks.spark.avro.hack.SchemaConvertersBackdoor
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType

object DatabricksAvro extends TemporalProjectedExtentCodec {

  def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]

  def main(args: Array[String]): Unit = {

    val tpe = TemporalProjectedExtent(Extent(0.0, 0.0, 8.0, 10.0), LatLng, ZonedDateTime.now())

    val (t1, t2, t3) = (ByteConstantTile(1, 10, 10), ByteConstantTile(2, 10, 10), ByteConstantTile(3, 10, 10))

    val tile = MultibandTile(
      ByteArrayTile(t1.toBytes(), 10, 10),
      ByteArrayTile(t2.toBytes(), 10, 10),
      ByteArrayTile(t3.toBytes(), 10, 10)
    )

    val target = (tpe, tile)

    val codec: AvroRecordCodec[(TemporalProjectedExtent, MultibandTile)] = codecOf(target)

    println(s"codec.schema.toString(true): ${codec.schema.toString(true)}")

    println("*" * 50)

    val sqlType = SchemaConverters.toSqlType(codec.schema)

    println(s"sqlType: $sqlType")
    println(s"sqlType.dataType.prettyJson: ${sqlType.dataType.prettyJson}")


    def makeRowEncoder[T](codec: AvroRecordCodec[T]) = {
      val sqlType = SchemaConverters.toSqlType(codec.schema)
      val structType = sqlType.dataType match {
        case st: StructType ⇒ st
        case _ ⇒ throw new IllegalArgumentException(s"${sqlType.dataType} not a struct")
      }

      val avroEncoder: (T) ⇒ GenericRecord = codec.encode

      val structTranslator = SchemaConvertersBackdoor.createConverterToSQL(codec.schema, structType)

      val cast = (a: AnyRef) ⇒ a.asInstanceOf[GenericRow]

      val rowEncoder = RowEncoder(structType)

      avroEncoder andThen structTranslator andThen cast andThen rowEncoder.toRow
    }

    val encoder = makeRowEncoder(codec)

    val at = encoder(target)
    println("at: " + at)



//    val spark = SparkSession.builder()
//      .master("local[2]")
//      .appName("AvroReadBenchmark")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val sc = spark.sparkContext
//
//    val gtrdd = sc.makeRDD(Seq(target))
//
//    val gtds = gtrdd.map(encoder).toDF
//
//    gtds.show()
//
//    spark.stop()
  }
}
