package astraea.demo

import com.databricks.spark.avro.SchemaConverters
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BaseGenericInternalRow, BoundReference, Literal, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.reflect.ClassTag

/**
 * Spark SQL [[Encoder]] derived from an existing GeoTrellis [[AvroRecordCodec]].
 *
 * @author sfitch 
 * @since 2/7/17
 */
object AvroDerivedSparkEncoder {

  def apply[T: AvroRecordCodec: ClassTag]: Encoder[T] = {

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
      // Now wondering if `flat` might mean that a single column is expanded into multiple ones?
      // ~~Assuming "flat" means all columns are primitive types. Not really sure.~~
      flat = sparkSchema.fields.length == 1 && !sparkSchema.fields.exists(_.dataType.isInstanceOf[StructType]),
      serializer = {
        val br = BoundReference(0, sparkDataType[T], false)
        sparkSchema.fields.map(f ⇒ EncodeUsingAvro(br, f, codec))
      },
      deserializer = Literal(null),
      clsTag = implicitly[ClassTag[T]])
  }


  class InternalAvroRow(avroRecord: GenericRecord, sparkType: DataType) extends BaseGenericInternalRow {
    override protected def genericGet(ordinal: Int): Any = {
      val value = avroRecord.get(ordinal)
      value
    }

    override def numFields: Int = sparkType match {
      case struct: StructType ⇒ struct.fields.length
      case _ ⇒ 1
    }

    override def copy(): InternalRow = this
  }

  case class EncodeUsingAvro[T: ClassTag](
    child: BoundReference, sparkSchema: StructField, codec: AvroRecordCodec[T])
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override def dataType: DataType = sparkSchema.dataType

    override def nullable: Boolean = false

    override def eval(input: InternalRow): Any = {
      if (input.isNullAt(child.ordinal)) {
        null
      }
      else {
        val obj = input.get(child.ordinal, null).asInstanceOf[T]
        val avroRecord = codec.encode(obj)

        dataType match {
          case s: StructType ⇒ new InternalAvroRow(avroRecord.get(sparkSchema.name).asInstanceOf[GenericRecord], sparkSchema.dataType)
          case o ⇒ avroRecord.get(sparkSchema.name)
        }



        //avro2spark(avroEncoded, sparkSchema)

        //val converter = SchemaConvertersBackdoor.createConverterToSQL(codec.schema, sparkSchema)
        //val genericRow = converter(avroEncoded).asInstanceOf[GenericRow]
        //val sqlRowlEncoder = RowEncoder(sparkSchema)

        // XXX Data corruption happening here in writing to `UnsafeRow`.
        //sqlRowlEncoder.toRow(genericRow)

      }
    }
  }

  /** Hack to get spark to create an `ObjectType` for us. */
  private def sparkDataType[T: ClassTag]: DataType = {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    val clsTag = implicitly[ClassTag[T]]
    DslSymbol(Symbol(clsTag.toString())).obj(clsTag.runtimeClass).dataType
  }

}
