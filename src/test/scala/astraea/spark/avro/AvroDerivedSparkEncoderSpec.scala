package astraea.spark.avro

import astraea.spark.TestEnvironment
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.spark.io.avro.codecs.{KeyValueRecordCodec, TupleCodec}
import geotrellis.spark.io.avro.{AvroRecordCodec, AvroUnionCodec}
import geotrellis.spark.{SpaceTimeKey, SpatialKey, TemporalProjectedExtent}
import geotrellis.vector.{Extent, ProjectedExtent}
import geotrellis.vectortile.VectorTile
import geotrellis.vectortile.protobuf.ProtobufTile
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.{FunSpec, Inspectors, Matchers}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import java.time.ZonedDateTime

import geotrellis.raster

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Test rig for [[AvroDerivedSparkEncoder]].
 * @author sfitch (@metasim)
 * @since 2/13/17
 */
class AvroDerivedSparkEncoderSpec extends FunSpec with Matchers with Inspectors with TestEnvironment {

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

      implicit val enc = encoderOf[StringWrapper]
      val example = StringWrapper(util.Random.nextString(10))
      val ds = sc.makeRDD(Seq(example)).toDS()

      assert(ds.toDF().head().getAs[Row](0).get(0) === example.payload)

      withClue("decoding") {
        assert(ds.head() === example)
      }
    }

    it("should handle map types") {
      roundTrip(hm, HazMap.Codec)
      implicit val enc = encoderOf[HazMap]

      val ds = sc.makeRDD(Seq(hm)).toDS

      assert(ds.select("HazMap.payload").head().getAs[Map[String, String]](0) === hm.payload)

      withClue("decoding") {
        assert(ds.head() === hm)
      }
    }

    it("should handle union of wrappers") {
      val ex1 = DoubleWrapper(55.6)
      val ex2 = StringWrapper("nanunanu")
      roundTrip(ex1, Wrapper.StringOrDoubleCodec)
      roundTrip(ex2, Wrapper.StringOrDoubleCodec)

      implicit val enc = encoderOf[Wrapper]

      val ds = rddToDatasetHolder(sc.makeRDD(Seq[Wrapper](ex1, ex2)))(enc).toDS()

      withClue("decoding") {
        assert(ds.collect().collect { case d: DoubleWrapper ⇒ d}.head === ex1)
      }
    }

    it("should handle Extent") {

      implicit val enc = encoderOf[Extent]

      val ds = sc.makeRDD(Seq(extent)).toDS

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

      val field = ds(ds.columns.head).getItem("instant")

      assert(ds.select(field).as[Double].head === stk.instant)


      withClue("decoding") {
        assert(ds.head() === stk)
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


    it("should handle ProjectedExtent") {
      implicit val enc = encoderOf[ProjectedExtent]

      val ds = sc.makeRDD(Seq(pe)).toDS

      val field1 = ds(ds.columns.head).getItem("epsg")

      assert(ds.select(field1).as[Double].head === tpe.crs.epsgCode.get)

      val field2 = ds(ds.columns.head).getItem("extent").getItem("ymax")

      assert(ds.select(field2).as[Double].head === tpe.extent.ymax)

      withClue("decoding"){
        assert(ds.head() === pe)
      }
    }

    it("should handle KeyValueRecordCodec") {
      implicit val avroKV = KeyValueRecordCodec[SpatialKey, DoubleWrapper]
      implicit val enc = encoderOf[Vector[(SpatialKey, DoubleWrapper)]]

      val keyVals = Vector((sk, oneThird), (SpatialKey(99, 100), DoubleWrapper(1.0)))
      val ds = sc.makeRDD(Seq(keyVals)).toDS

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
      withClue("decoding") {
        assert(ds.filter(_.rows == 2).head() === constantTile)
      }
    }

    it("should handle ByteArrayTile") {
      implicit val enc = encoderOf[ByteArrayTile]

      val ds = sc.makeRDD(Seq(byteArrayTile)).toDS

      assert(ds.map(_.asciiDraw()).head() === byteArrayTile.asciiDraw())

      withClue("decoding") {
        assert(ds.head() === byteArrayTile)
      }
    }

    it("should handle all Tile types") {
      implicit val enc = encoderOf[Tile]

      val ds = sc.makeRDD(allTileTypes).toDS

      withClue("decoding") {
        val decoded = ds.collect()
        forAll(decoded.zip(allTileTypes)) {
          case (result, expected) ⇒ assert(result.asciiDraw() === expected.asciiDraw())
        }
      }
    }

    it("should handle generic Tiles") {
      implicit val enc = encoderOf[Tile]

      val ds = sc.makeRDD(Seq[Tile](byteArrayTile, constantTile)).toDS

      withClue("decoding") {
        assert(ds.map(_.asciiDraw()).collect() === Array(byteArrayTile.asciiDraw(), constantTile.asciiDraw()))
      }
    }

    it("should handle TileFeature") {
      implicit val enc = encoderOf[TileFeature[BitConstantTile, StringWrapper]]

      val ds = sc.makeRDD(Seq(tileFeature)).toDS

      withClue("decoding") {
        assert(ds.map(_.data.payload).head() === tileFeature.data.payload)
      }
    }

    it("should handle VectorTile") {
      implicit val enc = encoderOf[VectorTile]
      val ds = sc.makeRDD(Seq(vectorTile)).toDS

      assert(ds.select("VectorTile.extent.ymax").as[Double].head() === extent.ymax)
      assert(ds.select("VectorTile.bytes").as[Array[Byte]].head() === vectorTile.asInstanceOf[ProtobufTile].toBytes)

      withClue("decoding") {
        assert(ds.head() === vectorTile)
      }
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
  val pe = ProjectedExtent(extent, LatLng)
  val tpe = TemporalProjectedExtent(pe, instant)
  val hm = HazMap(Map("foo" -> "bar", "baz" -> "boom"))

  val byteArrayTile = ByteArrayTile((1 to 9).map(_ .toByte).toArray, 3, 3)

  val constantTile = BitConstantTile(1, 2, 2)
  val tileFeature = TileFeature(constantTile, StringWrapper("beepbob"))

  val allTileTypes: Seq[Tile] = {
    val rows = 3
    val cols = 3
    val range = 1 to rows * cols
    def rangeArray[T: ClassTag](conv: (Int ⇒ T)): Array[T] = range.map(conv).toArray
    Seq(
      BitArrayTile(Array[Byte](0,1,2,3,4,5,6,7,8), 3*8, 3),
      ByteArrayTile(rangeArray(_.toByte), rows, cols),
      DoubleArrayTile(rangeArray(_.toDouble), rows, cols),
      FloatArrayTile(rangeArray(_.toFloat), rows, cols),
      IntArrayTile(rangeArray(identity), rows, cols),
      ShortArrayTile(rangeArray(_.toShort), rows, cols),
      UByteArrayTile(rangeArray(_.toByte), rows, cols),
      UShortArrayTile(rangeArray(_.toShort), rows, cols)
    )
  }

  lazy val vectorTile = ProtobufTile.fromBytes(
    IOUtils.toByteArray(getClass.getResourceAsStream("/MultiLayer.mvt")),
    extent
  )

  def encoderOf[T: AvroRecordCodec: TypeTag] =
    AvroDerivedSparkEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  // Simple wrapper types and associated codecs for testing basic union ops.
  trait Wrapper
  object Wrapper {
    implicit object StringOrDoubleCodec extends AvroUnionCodec[Wrapper](StringWrapper.Codec, DoubleWrapper.Codec)
  }

  case class StringWrapper(payload: String) extends Wrapper
  object StringWrapper {
    implicit object Codec extends AvroRecordCodec[StringWrapper] {
      // codec without namespace
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
    // codec with namespace
    implicit object Codec extends AvroRecordCodec[DoubleWrapper] {
      override def schema: Schema = SchemaBuilder
        .record("DoubleWrapper").namespace("astraea.spark.avro")
        .fields()
        .name("payload").`type`.doubleType().noDefault()
        .endRecord()

      override def encode(thing: DoubleWrapper, rec: GenericRecord): Unit =
        rec.put("payload", thing.payload)

      override def decode(rec: GenericRecord): DoubleWrapper =
        DoubleWrapper(rec.get("payload").asInstanceOf[Double])
    }
  }

  case class HazMap(payload: Map[String, String]) extends Wrapper
  object HazMap {
    implicit object Codec extends AvroRecordCodec[HazMap] {
      override def schema: Schema = SchemaBuilder
        .record("HazMap")
        .fields()
        .name("payload").`type`().map().values().stringType().noDefault()
        .endRecord()

      override def encode(thing: HazMap, rec: GenericRecord): Unit =
        rec.put("payload", thing.payload.asJava)


      override def decode(rec: GenericRecord): HazMap =
        HazMap(rec.get("payload").asInstanceOf[java.util.Map[String, String]].toMap)
    }
  }

}
