/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package astraea.spark.avro

import astraea.spark.TestEnvironment
import geotrellis.raster.{ByteCellType, Tile}
import org.apache.spark.sql.GTSQL
import org.scalatest.{FunSpec, Inspectors, Matchers}

/**
 * Test rig for Spark UDTs and friends for GT.
 * @author sfitch 
 * @since 3/30/17
 */
class GTSQLSpec extends FunSpec with Matchers with Inspectors with TestEnvironment {

  GTSQL.init(sql)

  describe("GeoTrellis UDTs") {
    it("should create constant tiles") {
      val query = sql.sql("select st_makeConstantTile(1, 10, 10, 'int8raw')")
      query.show(false)
      val tile = query.collect().head.getAs[Tile](0)
      assert(tile.cellType === ByteCellType)

      println(tile.asciiDrawDouble())
    }
    it("should evaluate UDF on tile") {
      val query = sql.sql("select st_focalSum(st_makeConstantTile(1, 10, 10, 'int8raw'), 4)")
      query.show(false)
      val tile = query.collect().head.getAs[Tile](0)
      assert(tile.cellType === ByteCellType)

      println(tile.asciiDrawDouble())
    }
    it("should generate multiple rows") {
      val query = sql.sql("select st_makeTiles(3)")
      query.show(false)
    }
    it("should expand rows") {
      val query = sql.sql("select st_explodeTile(st_makeConstantTile(1, 10, 10, 'int8raw'), st_makeConstantTile(2, 10, 10, 'int8raw'))")
      query.show(false)
    }
  }
}
