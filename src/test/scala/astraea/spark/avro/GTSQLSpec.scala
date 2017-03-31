package astraea.spark.avro

import astraea.spark.TestEnvironment
import geotrellis.raster.Tile
import org.apache.spark.sql.GTSQL
import org.scalatest.{FunSpec, Inspectors, Matchers}

/**
 *
 * @author sfitch 
 * @since 3/30/17
 */
class GTSQLSpec extends FunSpec with Matchers with Inspectors with TestEnvironment {

  GTSQL.init(sql)

  describe("GeoTrellis UDTs") {
    it("should create constant tiles") {
      val query = sql.sql("select st_makeConstantTile(7.0, 10, 10, 'float32raw')")
      query.printSchema
      val tile = query.collect().head.getAs[Tile](0)

      println(tile.asciiDrawDouble())
    }
  }
}
