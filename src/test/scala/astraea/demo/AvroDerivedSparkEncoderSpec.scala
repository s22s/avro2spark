package astraea.demo

import java.time.ZonedDateTime

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteArrayTile, Tile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.AvroRecordCodec
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers, Outcome}
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 *
 * @author sfitch 
 * @since 2/13/17
 */
class AvroDerivedSparkEncoderSpec extends FunSpec
  with Matchers with TestEnvironment with TemporalProjectedExtentCodec {
  import sql.implicits._

  // Test values
  val extent = Extent(1, 2, 3, 4)
  val tpe = TemporalProjectedExtent(extent, LatLng, ZonedDateTime.now())
  val tile: Tile = ByteArrayTile((1 to 9).map(_ .toByte).toArray, 3, 3)

  def encoderOf[T: AvroRecordCodec: TypeTag] =
    AvroDerivedSparkEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  describe("Avro-derived encoding") {
    it("should handle Extent") {

      implicit val enc = encoderOf[Extent]

      val ds = sc.makeRDD(Seq(extent)).toDS

      ds.show(false)

      val field = ds(ds.columns.head).getItem("xmax")

      assert(ds.select(field).as[Double].head() === extent.xmax)

      // When deserilizers are implemented.
      //assert(ds.map(_.xmax).head() === 3.0)
//      val extIn = df.rdd.collect().head
//      assert(extent === extIn)
    }


    it("should handle TemporalProjectedExtent") {
      implicit val enc = encoderOf[TemporalProjectedExtent]

      val ds = sc.makeRDD(Seq(tpe)).toDS

      ds.printSchema()
      ds.show(false)

      val field1 = ds(ds.columns.head).getItem("epsg")

      assert(ds.select(field1).as[Double].head === tpe.crs.epsgCode.get)

      val field2 = ds(ds.columns.head).getItem("extent").getItem("ymax")

      assert(ds.select(field2).as[Double].head === tpe.extent.ymax)
    }

    it("should handle Tile") {
      implicit val enc = encoderOf[Tile]

      val ds = sc.makeRDD(Seq(tile)).toDS

      ds.printSchema()
      ds.show(false)

    }

  }
}
