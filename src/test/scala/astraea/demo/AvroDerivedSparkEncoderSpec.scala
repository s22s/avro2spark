package astraea.demo

import java.time.ZonedDateTime

import geotrellis.proj4.LatLng
import geotrellis.raster.{BitConstantTile, ByteArrayTile, Tile, TileFeature}
import geotrellis.spark.{SpaceTimeKey, SpatialKey, TemporalProjectedExtent}
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.spark.io.avro.codecs.KeyValueRecordCodec
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
class AvroDerivedSparkEncoderSpec extends FunSpec with Matchers with TestEnvironment {

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

      implicit val enc = encoderOf[Wrapper]

      val ds = rddToDatasetHolder(sc.makeRDD(Seq[Wrapper](ex1, ex2)))(enc).toDF()
      ds.show(false)

      // TODO: Write test when unions work.
      assert(false)
    }

    it("should handle Extent") {

      implicit val enc = encoderOf[Extent]

      val ds = sc.makeRDD(Seq(extent)).toDS

      ds.show(false)

      val field = ds(ds.columns.head).getItem("xmax")

      assert(ds.select(field).as[Double].head() === extent.xmax)

      withClue("decoding") {
        assert(ds.head() === extent)
        assert(ds.map(_.xmax).head() === 3.0)
        val extIn = ds.rdd.collect().head
        assert(extent === extIn)
      }
    }

    it("should handle SpatialKey") {
      implicit val enc = encoderOf[SpatialKey]

      val ds = sc.makeRDD(Seq(sk)).toDS

      val field = ds(ds.columns.head).getItem("row")

      assert(ds.select(field).as[Double].head === sk.row)

      withClue("decoding") {
        ds.head() === sk
      }
    }

    it("should handle SpaceTimeKey") {
      implicit val enc = encoderOf[SpaceTimeKey]

      val ds = sc.makeRDD(Seq(stk)).toDS

      ds.printSchema()
      ds.show(false)

      val field = ds(ds.columns.head).getItem("instant")

      assert(ds.select(field).as[Double].head === stk.instant)

      withClue("decoding") {
        ds.head() === stk
      }
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

    it("should handle KeyValueRecordCodec") {
      implicit val avroKV = KeyValueRecordCodec[SpatialKey, DoubleWrapper]
      implicit val enc = encoderOf[Vector[(SpatialKey, DoubleWrapper)]]

      val keyVals = Vector((sk, oneThird), (SpatialKey(99, 100), DoubleWrapper(1.0)))
      val ds = sc.makeRDD(Seq(keyVals)).toDS

      ds.printSchema()
      ds.show(false)

      assert(ds.selectExpr("Vector.pairs[0]._2.payload").as[Double].head === oneThird.payload)

      withClue("decoding") {
        assert(ds.head() === keyVals)
      }
    }

    it("should handle constant Tile") {
      // We have to force the use of our encoder in this case because `ConstantTile`s
      // are `Product` types, which already have `Encoder`s.
      val enc = encoderOf[BitConstantTile]
      val ds = rddToDatasetHolder(sc.makeRDD(Seq(constantTile)))(enc).toDS
      assert(ds.filter(_.rows == 2).head() === constantTile)
    }

    it("should handle ByteArrayTile") {
      implicit val enc = encoderOf[ByteArrayTile]

      val ds = sc.makeRDD(Seq(arrayTile)).toDS

      assert(ds.map(_.asciiDraw()).head() === arrayTile.asciiDraw())
    }

    it("should handle generic Tile") {
      implicit val enc = encoderOf[Tile]

      // TODO: This fails because unions fail.

      val ds = sc.makeRDD(Seq(arrayTile: Tile)).toDS

      ds.show(false)

      assert(ds.map(_.asciiDraw()).head() === arrayTile.asciiDraw())
    }

    it("should handle TileFeature") {
      implicit val enc = encoderOf[TileFeature[BitConstantTile, StringWrapper]]

      val ds = sc.makeRDD(Seq(tileFeature)).toDS

      assert(ds.map(_.data.payload).head() === tileFeature.data.payload)
    }
  }
}

/** Test support bits */
object AvroDerivedSparkEncoderSpec {
  val oneThird = DoubleWrapper(1.0/3.0)
  val instant = ZonedDateTime.now()
  val extent = Extent(1, 2, 3, 4)
  val sk = SpatialKey(37, 41)
  val stk = SpaceTimeKey(sk, instant)
  val tpe = TemporalProjectedExtent(extent, LatLng, instant)

  val arrayTile = ByteArrayTile((1 to 9).map(_ .toByte).toArray, 3, 3)

  val constantTile = BitConstantTile(1, 2, 2)
  val tileFeature = TileFeature(constantTile, StringWrapper("beepbob"))

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
