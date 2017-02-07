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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericRow, LeafExpression, Literal, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object DatabricksAvro extends TemporalProjectedExtentCodec {
  case class EncodeUsingAvro[T: ClassTag](child: BoundReference, schema: StructType, codec: AvroRecordCodec[T]) extends UnaryExpression with NonSQLExpression with CodegenFallback {
    override def dataType: DataType = schema

    override def nullable: Boolean = false

    override def eval(input: InternalRow): Any = {
      if (input.isNullAt(child.ordinal)) {
        null
      }
      else {
        val obj = input.get(child.ordinal, null).asInstanceOf[T]
        val encoded = codec.encode(obj)
        val converter = SchemaConvertersBackdoor.createConverterToSQL(codec.schema, schema)
        InternalRow.fromSeq(converter(encoded).asInstanceOf[GenericRow].toSeq)
      }
    }
  }

  private def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]


  object AvroDerivedEncoder {
    def apply[T: AvroRecordCodec: ClassTag: TypeTag]: Encoder[T] = {

      val codec = implicitly[AvroRecordCodec[T]]
      val sparkSchema = {
        val sqlType = SchemaConverters.toSqlType(codec.schema)
        sqlType.dataType match {
          case st: StructType ⇒ st
          case _ ⇒ throw new IllegalArgumentException(s"${sqlType.dataType} not a struct")
        }
      }

      sparkSchema.printTreeString()

      // Assuming "flat" means all columns are primitive types.
      val flat = !sparkSchema.fields.exists(_.dataType.isInstanceOf[StructType])
      val clsTag = implicitly[ClassTag[T]]
      val serializer: Seq[Expression] = {
        val br = BoundReference(0, sparkSchema, false)
        // need to figure out how to use the codec.encode method via expression

        Seq(EncodeUsingAvro(br, sparkSchema, codec))

      }
      val deserializer: Expression =  Literal("dummy")
      ExpressionEncoder[T](sparkSchema, flat, serializer, deserializer, clsTag)
    }
  }

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
      .master("local[1]")
      .appName("avro2spark")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .getOrCreate()

    import spark.implicits._

    @transient
    val sc = spark.sparkContext

    val gtrdd = sc.makeRDD(Seq(target))

    implicit val crsEncoder = AvroDerivedEncoder[TemporalProjectedExtent]

    val gtdf = gtrdd.map(_._1).toDS

    gtdf.printSchema()
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
