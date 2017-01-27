package com.databricks.spark.avro.hack

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType

/**
 *
 * @author sfitch 
 * @since 1/27/17
 */
object SchemaConvertersBackdoor {
  def createConverterToSQL(
    sourceAvroSchema: Schema,
    targetSqlType: DataType): AnyRef => AnyRef = SchemaConverters.createConverterToSQL(sourceAvroSchema, targetSqlType)
}
