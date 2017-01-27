package astraea.demo

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteConstantTile, MultibandTile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent

import com.databricks.spark.avro.SchemaConverters
import java.time.ZonedDateTime

object DatabricksAvro extends TemporalProjectedExtentCodec {

  def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]

  def main(args: Array[String]): Unit = {

    val tpe = TemporalProjectedExtent(Extent(0.0, 0.0, 8.0, 10.0), LatLng, ZonedDateTime.now())

    val tile = MultibandTile(
      ByteConstantTile(1, 10, 10),
      ByteConstantTile(2, 10, 10),
      ByteConstantTile(3, 10, 10)
    )

    val target = (tpe, tile)

    val codec = codecOf(target)

    println(s"codec.schema.toString(true): ${codec.schema.toString(true)}")

    println("*" * 50)

    val sqlType = SchemaConverters.toSqlType(codec.schema)

    println(s"sqlType: $sqlType")
    println(s"sqlType.dataType.prettyJson: ${sqlType.dataType.prettyJson}")
  }
}
