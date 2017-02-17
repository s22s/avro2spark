package astraea.demo

import java.time.ZonedDateTime

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteArrayTile, Tile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.spark.io.avro.{AvroRecordCodec, AvroUnionCodec}
import geotrellis.vector.Extent
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.{FunSpec, Matchers}

import scala.reflect.runtime.universe._

/**
 * Test rig for [[AvroDerivedSparkEncoder]].
 * @author sfitch 
 * @since 2/13/17
 */
class AvroDerivedSparkEncoderSpec extends FunSpec
  with Matchers with TestEnvironment with TemporalProjectedExtentCodec {

  import AvroDerivedSparkEncoderSpec._
  import sql.implicits._

  def roundTrip[T](value: T, codec: AvroRecordCodec[T]) =
    assert(codec.decode(codec.encode(value)) === value)


  describe("Avro-derived coding") {
    it("should handle double wrapping") {
      roundTrip(DoubleWrapper(47.56), DoubleWrapper.Codec)
      implicit val enc = encoderOf[DoubleWrapper]
      val example = DoubleWrapper(util.Random.nextGaussian())

      val ds = sc.makeRDD(Seq(example)).toDS()
      assert(ds.toDF().head().getAs[Row](0).get(0) === example.payload)

      withClue("decoding") {
        assert(ds.head() === example)
      }
    }

    it("should handle string wrapping") {
      roundTrip(StringWrapper("foobarbaz"), StringWrapper.Codec)

      // Now through Spark.
      implicit val enc = encoderOf[StringWrapper]
      val example = StringWrapper(util.Random.nextString(10))
      val ds = sc.makeRDD(Seq(example)).toDS()

      assert(ds.toDF().head().getAs[Row](0).get(0) === example.payload)

      withClue("decoding") {
        assert(ds.head() === example)
      }
    }

    it("should handle union of wrappers") {
      val ex1 = DoubleWrapper(55.6)
      val ex2 = StringWrapper("nanunanu")
      roundTrip(ex1, Wrapper.StringOrDoubleCodec)
      roundTrip(ex2, Wrapper.StringOrDoubleCodec)

      // Explicit type parameters necessary, otherwise `Encoder[Product]` will be resolved.
      implicit val enc = encoderOf[Wrapper]//(StringOrDoubleCodec, typeTag[Wrapper])

      val ds = rddToDatasetHolder(sc.makeRDD(Seq[Wrapper](ex1, ex2)))(enc).toDF()
      ds.show(false)
    }

    it("should handle Extent") {

      implicit val enc = encoderOf[Extent]

      val ds = sc.makeRDD(Seq(extent)).toDS

      ds.show(false)

      val field = ds(ds.columns.head).getItem("xmax")

      assert(ds.select(field).as[Double].head() === extent.xmax)

      withClue("decoding") {
        assert(ds.head() === extent)
      }

      // When deserilizers are implemented.
      //assert(ds.map(_.xmax).head() === 3.0)
//      val extIn = df.rdd.head()
//      assert(extent === extIn)
    }


    it("should handle TemporalProjectedExtent") {
      implicit val enc = encoderOf[TemporalProjectedExtent]

      val ds = sc.makeRDD(Seq(tpe)).toDS

      val field1 = ds(ds.columns.head).getItem("epsg")

      assert(ds.select(field1).as[Double].head === tpe.crs.epsgCode.get)

      val field2 = ds(ds.columns.head).getItem("extent").getItem("ymax")

      assert(ds.select(field2).as[Double].head === tpe.extent.ymax)

      withClue("decoding"){
        assert(ds.head() === tpe)
      }
    }

    it("should handle Tile") {
      implicit val enc = encoderOf[Tile]

      val ds = sc.makeRDD(Seq(tile)).toDS

      ds.printSchema()
      ds.show(false)

      println(ds.head())
    }
  }
}

object AvroDerivedSparkEncoderSpec {
  // Test values
  val extent = Extent(1, 2, 3, 4)
  val tpe = TemporalProjectedExtent(extent, LatLng, ZonedDateTime.now())
  val tile: Tile = ByteArrayTile((1 to 9).map(_ .toByte).toArray, 3, 3)

  def encoderOf[T: AvroRecordCodec: TypeTag] =
    AvroDerivedSparkEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  trait Wrapper
  object Wrapper {
    implicit object StringOrDoubleCodec extends AvroUnionCodec[Wrapper](StringWrapper.Codec, DoubleWrapper.Codec)
  }

  case class StringWrapper(payload: String) extends Wrapper
  object StringWrapper {
    implicit object Codec extends AvroRecordCodec[StringWrapper] {
      override def schema: Schema = SchemaBuilder
        .record("StringWrapper")
        .fields()
        .name("payload").`type`.stringType().noDefault()
        .endRecord()

      override def encode(thing: StringWrapper, rec: GenericRecord): Unit =
        rec.put("payload", thing.payload)

      override def decode(rec: GenericRecord): StringWrapper =
        StringWrapper(rec.get("payload").asInstanceOf[String])
    }
  }

  case class DoubleWrapper(payload: Double) extends Wrapper
  object DoubleWrapper {
    implicit object Codec extends AvroRecordCodec[DoubleWrapper] {
      override def schema: Schema = SchemaBuilder
        .record("DoubleWrapper")
        .fields()
        .name("payload").`type`.doubleType().noDefault()
        .endRecord()

      override def encode(thing: DoubleWrapper, rec: GenericRecord): Unit =
        rec.put("payload", thing.payload)

      override def decode(rec: GenericRecord): DoubleWrapper =
        DoubleWrapper(rec.get("payload").asInstanceOf[Double])
    }
  }
}
