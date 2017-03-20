package astraea.spark.avro

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

case class SchemaType(dataType: DataType, nullable: Boolean)

object SchemaType {
  def fromAvro(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case INT => SchemaType(IntegerType, nullable = false)
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES => SchemaType(BinaryType, nullable = false)
      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => SchemaType(LongType, nullable = false)
      case FIXED => SchemaType(BinaryType, nullable = false)
      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = fromAvro(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = fromAvro(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = fromAvro(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            fromAvro(remainingUnionTypes.head).copy(nullable = true)
          } else {
            fromAvro(Schema.createUnion(remainingUnionTypes.asJava)).copy(nullable = true)
          }
        } else avroSchema.getTypes.asScala.map(_.getType) match {
          case Seq(t1) =>
            fromAvro(avroSchema.getTypes.get(0))
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case _ =>
            // Convert complex unions to struct types where field names are either names of primitives
            //  or names of the avro record. Avro union spec does not allow ambiguity that would break this.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.map { schema: Schema =>
              // All fields are nullable because only one of them is set at a time
              val ft = fromAvro(schema).dataType
              schema.getType match {
                case RECORD =>
                  StructField(schema.getName, ft, nullable = true)
                case avroType =>
                  StructField(avroType.getName, ft, nullable = true)
              }
            }

            SchemaType(StructType(fields), nullable = false)
        }

      case other => throw new IllegalArgumentException(s"Unsupported type ${other.toString}")
    }
  }
}
