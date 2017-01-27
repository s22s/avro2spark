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
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
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
    val codec = codecOf(target)

    println(s"codec.schema.toString(true): ${codec.schema.toString(true)}")

    println("*" * 50)

    val sqlType = SchemaConverters.toSqlType(codec.schema)

    def makeRowEncoder[T](codec: AvroRecordCodec[T]) = {
      val sqlType = SchemaConverters.toSqlType(codec.schema)
      println(s"sqlType.dataType.prettyJson: ${sqlType.dataType.prettyJson}")

      val structType = sqlType.dataType match {
        case st: StructType ⇒ st
        case _ ⇒ throw new IllegalArgumentException(s"${sqlType.dataType} not a struct")
      }

      val avroEncoder: (T) ⇒ GenericRecord = codec.encode

      val structTranslator = SchemaConvertersBackdoor.createConverterToSQL(codec.schema, structType)

      val cast = (a: AnyRef) ⇒ a.asInstanceOf[Row]

      avroEncoder andThen structTranslator andThen cast
    }

    val encoder = makeRowEncoder(codec)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("avro2spark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .getOrCreate()

    import spark.implicits._

    @transient
    val sc = spark.sparkContext

    val gtrdd = sc.makeRDD(Seq(target))

    val trans: RDD[Row] = gtrdd.map { p ⇒
      val encoder = makeRowEncoder(codec)
      encoder(p)
    }



    val gtdf = spark.createDataFrame(trans , sqlType.dataType.asInstanceOf[StructType])

    gtdf.show(false)

    spark.stop()
  }
}
