package astraea.spark

import com.databricks.spark.avro.SchemaConverters
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.spark.sql.types._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.collection.JavaConversions._
import scala.util.Try

/**
 * Supporting cast of characters in Avro <-> Catalyst conversion.
 *
 * @author sfitch (@metasim)
 * @since 2/23/17
 */
package object avro {
  /** Pairing of field name with it's order in the record. */
  private[avro] case class FieldSelector(ordinal: Int, name: String)

  /** Synchronized pairing of Spark and Avro schemas. */
  private[avro] case class SchemaPair(spark: DataType, avro: Schema) {

    /** Is an Avro union represented in this Schema. */
    val isUnion = avro.getType == Type.UNION
//
//    /** Attempt to find where in an Avro union the associated Spark type lives. */
//    def avroUnionIndexOfSparkType = Try(
//      avro.getTypes.indexWhere(s ⇒ avro2Spark.get(s.getType).contains(spark)))
//      .toOption
//      .filter(_ > -1)

    def sparkStruct = spark match {
      case st: StructType ⇒ st
      case ot ⇒ throw new IllegalArgumentException("Expected StructType, not " + ot)
    }

    /** When this is a union, select the sub-schema at the given index in the union ordering. */
    def selectUnion(idx: Int): SchemaPair = {
      require(isUnion)
      val s = sparkStruct.fields(idx)
      val a = avro.getTypes.get(idx)
      SchemaPair(s.dataType, a)
    }

    def elementType: SchemaPair = spark match {
      case at: ArrayType ⇒ SchemaPair(at.elementType, avro.getElementType)
      case ot ⇒ throw new IllegalArgumentException("Expected ArrayType, not " + ot)
    }

    def fields: Seq[(FieldSelector, SchemaPair)] = spark match {
      case st: StructType ⇒
        st.fields.zip(avro.getFields).zipWithIndex.map {
          case ((sparkField, avroField), index) ⇒
            (FieldSelector(index, sparkField.name), SchemaPair(sparkField.dataType, avroField.schema()))
        }
      case _ ⇒ Seq.empty
    }
  }

  /** Utility to build a ClassTag from a TypeTag. */
  private[avro] def classTagFor[T: TypeTag] = {
    val mirror = typeTag[T].mirror
    val tpe = typeTag[T].tpe
    val cls = mirror.runtimeClass(tpe)
    ClassTag[T](cls)
  }

  /** Via Databricks' `spark-avro`, convert the Avro schema into a Spark/Catalyst schema. */
  private[avro] def schemaFor[T: AvroRecordCodec]: StructType = {
    val codec = implicitly[AvroRecordCodec[T]]

    // Convert Avro schema to Spark schema via Databricks library
    val sqlType = SchemaConverters.toSqlType(codec.schema)

    sqlType.dataType match {
      case st: StructType ⇒ st
      case dt ⇒ throw new IllegalArgumentException(s"$dt not a struct. Use built-in Spark `Encoder`s instead.")
    }
  }

  private[avro] val avro2Spark = Map(
    Schema.Type.BOOLEAN -> BooleanType,
    Schema.Type.INT -> IntegerType,
    Schema.Type.LONG -> LongType,
    Schema.Type.FLOAT -> FloatType,
    Schema.Type.DOUBLE -> DoubleType,
    Schema.Type.BYTES -> BinaryType,
    Schema.Type.STRING -> StringType,
    Schema.Type.RECORD -> StructType,
    Schema.Type.ENUM -> StringType,
    Schema.Type.ARRAY -> ArrayType,
    Schema.Type.MAP -> MapType,
    Schema.Type.FIXED -> BinaryType,
    Schema.Type.UNION -> NullType // <-- Using NullType as a sentinel.
  )

  private[avro] val spark2Avro = Map(
    BooleanType -> Set(Schema.Type.BOOLEAN, Schema.Type.UNION),
    IntegerType -> Set(Schema.Type.INT, Schema.Type.UNION),
    LongType -> Set(Schema.Type.LONG, Schema.Type.UNION),
    FloatType -> Set(Schema.Type.FLOAT, Schema.Type.UNION),
    DoubleType -> Set(Schema.Type.DOUBLE, Schema.Type.UNION),
    BinaryType -> Set(Schema.Type.BYTES, Schema.Type.FIXED, Schema.Type.UNION),
    StringType -> Set(Schema.Type.STRING, Schema.Type.ENUM, Schema.Type.UNION),
    StructType -> Set(Schema.Type.RECORD, Schema.Type.UNION),
    ArrayType -> Set(Schema.Type.ARRAY, Schema.Type.UNION),
    MapType -> Set(Schema.Type.MAP, Schema.Type.UNION)
  )

  /** Generate a field name for the serialized object. Shows up as the default column name. */
  private[avro] def fieldNameFor[T: TypeTag]: String = classTagFor[T].runtimeClass.getSimpleName

}
