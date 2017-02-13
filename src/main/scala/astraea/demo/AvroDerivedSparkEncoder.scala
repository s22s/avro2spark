package astraea.demo

import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.avro.hack.SchemaConvertersBackdoor
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, GenericRow, Literal, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Spark SQL [[Encoder]] derived from an existing GeoTrellis [[AvroRecordCodec]].
 *
 * @author sfitch 
 * @since 2/7/17
 */
object AvroDerivedSparkEncoder {

  /** Main entry point for creating a Spark `Encoder` from an implicitly available `AvroRecordCodec`. */
  def apply[T: AvroRecordCodec: TypeTag]: Encoder[T] = {

    val cls = classTagFor[T]
    val schema = schemaFor[T]

    // Now wondering if `flat` might mean that a single column is expanded into multiple ones?
    // ~~Assuming "flat" means all columns are primitive types. Not really sure.~~
    val flat = schema.fields.length == 1 && !schema.fields.head.dataType.isInstanceOf[StructType]

    val inputObject = BoundReference(0, dataTypeFor[T], nullable = false)

    val serializer = serializerFor[T](inputObject, schema)
    //println(serializer.treeString(true))

    val deserializer = deserializerFor[T]

    val wrappedSchema = StructType(Seq(StructField(fieldNameFor[T], schema)))

    // As of Spark 2.1.0, the `ExpressionEncoder` API is the only publicly accessible means of creating
    // RDD => Dataframe converters. This means using the Catalyst `Expression` API, where an execution
    // plan is defined in AST-like form.
    ExpressionEncoder[T](
      wrappedSchema,
      flat,
      Seq(serializer),
      deserializer,
      cls
    )
  }

  /** Intermediate expression to convert an incoming expression of type `T` to an Avro [[GenericRecord]]. */
  case class EncodeToAvro[T: AvroRecordCodec](child: Expression)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override def dataType: DataType = EncodeToAvro.genericRecordType

    override def nullable: Boolean = false

    override protected def nullSafeEval(input: Any): GenericRecord = {
      val obj = input.asInstanceOf[T]
      val codec = implicitly[AvroRecordCodec[T]]
      //println(">>>>>>> Encoding: " + obj)
      codec.encode(obj)
    }

    override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil
  }

  object EncodeToAvro {
    val genericRecordType = dataTypeFor[GenericRecord]
  }

  case class AvroToSpark(child: Expression, sparkSchema: StructType)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override def dataType: DataType = sparkSchema

    def convertRow(genericRecord: GenericRecord, rowSchema: StructType): InternalRow = {

      def convertField(fieldValue: Any, sparkType: DataType): Any = {

        sparkType match {
          case nestedSchema: StructType ⇒
            val nestedRecord = fieldValue.asInstanceOf[GenericRecord]
            val converter = SchemaConvertersBackdoor.createConverterToSQL(nestedRecord.getSchema, nestedSchema)
            val genericRow = converter(nestedRecord).asInstanceOf[GenericRow]
            val sqlRowlEncoder = RowEncoder(nestedSchema)
            sqlRowlEncoder.toRow(genericRow)
          case arraySchema: ArrayType ⇒
            val nestedRecord = fieldValue.asInstanceOf[java.util.AbstractCollection[AnyRef]]
            val avroRecord = fieldValue.asInstanceOf[GenericRecord]

            // Get the schema of the element type

            val elementSchema = avroRecord.getSchema.getField("foobar").schema().getElementType

            val converter = SchemaConvertersBackdoor.createConverterToSQL(elementSchema, arraySchema.elementType)

            val caster = (a: AnyRef) ⇒ a.asInstanceOf[GenericRow]

            val transform = arraySchema.elementType match {
              case st: StructType ⇒
                converter andThen caster andThen RowEncoder(st).toRow
              case _ ⇒ converter
            }

            val result = nestedRecord.asScala.map(transform)

            new GenericArrayData(result)
          case _ ⇒
            //val converter = SchemaConvertersBackdoor.createConverterToSQL(avroRecord.getSchema, sparkSchema.dataType)
            //val result = converter(fieldValue)
            //println(fieldValue + " -> " + result)
            //result
            println(s"skipping ${fieldValue} of type ${sparkType}")
            fieldValue
        }
      }

      convertField(genericRecord, rowSchema).asInstanceOf[InternalRow]

      //InternalRow(sparkSchema.fields.map { f ⇒
//        convertField(genericRow.getAs[Any](f.name), f.dataType)
//      })
    }

    override protected def nullSafeEval(input: Any): Any = {
      val avroRecord = input.asInstanceOf[GenericRecord]

//      val converter = SchemaConvertersBackdoor.createConverterToSQL(avroRecord.getSchema, sparkSchema)
//      val genericRow = converter(avroRecord).asInstanceOf[GenericRow]
      convertRow(avroRecord, sparkSchema)
    }
  }

//  /** Expression for pulling a specific field out of */
//  case class ExtractFromAvro(child: Expression, sparkSchema: StructField)
//    extends UnaryExpression with NonSQLExpression with CodegenFallback {
//
//    override def checkInputDataTypes(): TypeCheckResult = {
//      child.dataType match {
//        //case e if e == EncodeToAvro.genericRecordType ⇒ TypeCheckResult.TypeCheckSuccess
//        case e if e == BinaryType ⇒ TypeCheckResult.TypeCheckSuccess
//        case _ ⇒ TypeCheckResult.TypeCheckFailure("Bad datatype: " + child.dataType)
//      }
//    }
//
//    override protected def nullSafeEval(input: Any): Any = {
//      val avroRecord = input.asInstanceOf[GenericRecord]
//
//      val fieldValue = avroRecord.get(sparkSchema.name)
//
//      dataType match {
//        case nestedSchema: StructType ⇒
//          val nestedRecord = fieldValue.asInstanceOf[GenericRecord]
//          val converter = SchemaConvertersBackdoor.createConverterToSQL(nestedRecord.getSchema, nestedSchema)
//          val genericRow = converter(nestedRecord).asInstanceOf[GenericRow]
//          val sqlRowlEncoder = RowEncoder(nestedSchema)
//          sqlRowlEncoder.toRow(genericRow)
//        case arraySchema: ArrayType ⇒
//          val nestedRecord = fieldValue.asInstanceOf[java.util.AbstractCollection[AnyRef]]
//
//          val elementSchema = avroRecord.getSchema.getField(sparkSchema.name).schema().getElementType
//
//          val converter = SchemaConvertersBackdoor.createConverterToSQL(elementSchema, arraySchema.elementType)
//
//          val caster = (a: AnyRef) ⇒ a.asInstanceOf[GenericRow]
//
//          val transform = arraySchema.elementType match {
//            case st: StructType ⇒
//              converter andThen caster andThen RowEncoder(st).toRow
//            case _ ⇒ converter
//          }
//
//          val result = nestedRecord.asScala.map(transform)
//
//          new GenericArrayData(result)
//        case _ ⇒
//          //val converter = SchemaConvertersBackdoor.createConverterToSQL(avroRecord.getSchema, sparkSchema.dataType)
//          //val result = converter(fieldValue)
//          //println(fieldValue + " -> " + result)
//          //result
//          fieldValue
//      }
//    }
//
//    override def dataType: DataType = sparkSchema.dataType
//
//    override def nullable: Boolean = false
//
//  }


  private def classTagFor[T: TypeTag] = {
    val mirror = typeTag[T].mirror
    val tpe = typeTag[T].tpe
    val cls = mirror.runtimeClass(tpe)
    ClassTag[T](cls)
  }


  /** Hack to get spark to create an `ObjectType` for us. */
//  private def sparkDataType[T: ClassTag]: DataType = {
//    import org.apache.spark.sql.catalyst.dsl.expressions._
//    val clsTag = implicitly[ClassTag[T]]
//    DslSymbol(Symbol(clsTag.toString())).obj(clsTag.runtimeClass).dataType
//  }

  /**
   * Returns the Spark SQL DataType for a given scala type.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned. Unlike `schemaFor`, this function doesn't do
   * any massaging of types into the Spark SQL type system.
   * As a result, ObjectType will be returned for things like boxed Integers
   * @see [[ScalaReflection.dataTypeFor]]
   */
  def dataTypeFor[T: TypeTag]: DataType = ScalaReflection.dataTypeFor[T]

  def schemaFor[T: AvroRecordCodec]: StructType = {
    val codec = implicitly[AvroRecordCodec[T]]

    // Convert Avro schema to Spark schema via Databricks library
    val sqlType = SchemaConverters.toSqlType(codec.schema)

    sqlType.dataType match {
      case st: StructType ⇒ st
      case dt ⇒ throw new IllegalArgumentException(s"$dt not a struct. Use built-in Spark `Encoder`s instead.")
    }
  }

  def fieldNameFor[T: TypeTag]: String = classTagFor[T].runtimeClass.getName

  def serializerFor[T: AvroRecordCodec: TypeTag](inputObject: Expression, schema: StructType): Expression = {
    val asAvro = EncodeToAvro(inputObject)
    val asSpark = AvroToSpark(asAvro, schema)
    CreateNamedStruct(Literal(fieldNameFor[T]) :: asSpark :: Nil)
  }

  // TODO: This definitely needs to be defined properly for encoder chaining to work (e.g. tuples or products)
  def deserializerFor[T : AvroRecordCodec]: Expression = Literal("TODO")

  // See:
  /*
  org.apache.spark.sql.catalyst.ScalaReflection#serializerFor
   */

}
