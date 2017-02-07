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
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, GenericRow, Literal, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object DatabricksAvro extends TemporalProjectedExtentCodec {
  case class EncodeUsingAvro[T: ClassTag](child: BoundReference, sparkSchema: StructType, codec: AvroRecordCodec[T]) extends UnaryExpression with NonSQLExpression with CodegenFallback {
    override def dataType: DataType = sparkSchema

    override def nullable: Boolean = false

    override def eval(input: InternalRow): Any = {
      if (input.isNullAt(child.ordinal)) {
        null
      }
      else {
        val obj = input.get(child.ordinal, null).asInstanceOf[T]
        val avroEncoded = codec.encode(obj)
        val converter = SchemaConvertersBackdoor.createConverterToSQL(codec.schema, sparkSchema)
        val genericRow = converter(avroEncoded).asInstanceOf[GenericRow]
        val sqlRowlEncoder = RowEncoder(sparkSchema)

        // XXX Data corruption happening here in writing to `UnsafeRow`.
        sqlRowlEncoder.toRow(genericRow)
      }
    }
  }

  object AvroDerivedEncoder {
    /** Hack to get spark to create an `ObjectType` for us. */
    def sparkDataType[T: ClassTag]: DataType = {
      import org.apache.spark.sql.catalyst.dsl.expressions._
      val clsTag = implicitly[ClassTag[T]]
      DslSymbol(Symbol(clsTag.toString())).obj(clsTag.runtimeClass).dataType
    }

    def apply[T: AvroRecordCodec: ClassTag: TypeTag]: Encoder[T] = {

      val codec = implicitly[AvroRecordCodec[T]]
      val sparkSchema = {
        val sqlType = SchemaConverters.toSqlType(codec.schema)
        sqlType.dataType match {
          case st: StructType ⇒ st
          case _ ⇒ throw new IllegalArgumentException(s"${sqlType.dataType} not a struct")
        }
      }


      ExpressionEncoder[T](
        schema = sparkSchema,
        // Assuming "flat" means all columns are primitive types. Not really sure.
        flat = !sparkSchema.fields.exists(_.dataType.isInstanceOf[StructType]),
        serializer = {
          //val sourceDataType: DataType = NullType // Want `ObjectType`, but it's private
          val br = BoundReference(0, sparkDataType[T], false)
          Seq(EncodeUsingAvro(br, sparkSchema, codec))
        },
        deserializer = Literal(null),
        clsTag = implicitly[ClassTag[T]])
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

    import org.apache.spark.sql.execution.debug._

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
