package astraea.demo

import geotrellis.proj4.CRS
import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector._

import org.apache.avro._
import org.apache.avro.generic._

// source: https://github.com/locationtech/geotrellis/blob/3bafc3f62abe325ea708b8a59ea551979fe23093/spark/src/main/scala/geotrellis/spark/io/avro/codecs/TemporalProjectedExtentCodec.scala

trait TemporalProjectedExtentCodec {
  implicit def temporalProjectedExtentCodec = new AvroRecordCodec[TemporalProjectedExtent] {
    def schema: Schema = SchemaBuilder
      .record("TemporalProjectedExtent").namespace("geotrellis.spark")
      .fields()
      .name("extent").`type`(extentCodec.schema).noDefault()
      .name("epsg").`type`().intType().noDefault()
      .name("instant").`type`().longType().noDefault()
      .endRecord()

    def encode(temporalProjectedExtent: TemporalProjectedExtent, rec: GenericRecord): Unit = {
      rec.put("extent", extentCodec.encode(temporalProjectedExtent.extent))
      rec.put("epsg", temporalProjectedExtent.crs.epsgCode.get)
      rec.put("instant", temporalProjectedExtent.instant)
    }

    def decode(rec: GenericRecord): TemporalProjectedExtent = {
      val instant = rec[Long]("instant")
      val epsg = rec[Int]("epsg")
      val crs = CRS.fromEpsgCode(epsg)

      val extent = extentCodec.decode(rec[GenericRecord]("extent"))

      TemporalProjectedExtent(extent, crs, instant)
    }
  }
}
