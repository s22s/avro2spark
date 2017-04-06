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

package astraea.spark

import geotrellis.raster.{ByteCellType, MultibandTile, Tile, TileFeature}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.GTSQL.Implicits._
import org.apache.spark.sql.GTSQLFunctions.flattenExtent
import org.apache.spark.sql.{DataFrame, GTSQL, GTSQLFunctions}
import org.scalatest.{FunSpec, Inspectors, Matchers}
import org.apache.spark.sql.execution.debug._

/**
 * Test rig for Spark UDTs and friends for GT.
 * @author sfitch
 * @since 3/30/17
 */
class GTSQLSpec extends FunSpec with Matchers with Inspectors with TestEnvironment with TestData {

  GTSQL.init(sql)

  implicit class DFExtras(df: DataFrame) {
    def firstTile: Tile = return df.collect().head.getAs[Tile](0)
  }

  import _spark.implicits._

  describe("GeoTrellis UDTs") {
    it("should create constant tiles") {
      val query = sql.sql("select st_makeConstantTile(1, 10, 10, 'int8raw')")
      val tile = query.firstTile
      assert((tile.cellType === ByteCellType) (org.scalactic.Equality.default))
    }

    it("should evaluate UDF on tile") {
      val query = sql.sql("select st_focalSum(st_makeConstantTile(1, 10, 10, 'int8raw'), 4)")
      val tile = query.firstTile
      assert(tile.cellType === ByteCellType)
      // println(tile.asciiDrawDouble())
    }

    it("should generate multiple rows") {
      val query = sql.sql("select st_makeTiles(3)")
      val tiles = query.collect().head.getAs[Seq[Tile]](0)
      assert(tiles.distinct.size == 1)

    }

    it("should expand rows") {
      val query = sql.sql("select st_explodeTile(st_makeConstantTile(1, 10, 10, 'int8raw'), st_makeConstantTile(2, 10, 10, 'int8raw'))")
      assert(query.as[(Double, Double)].collect().forall(_ == (1.0, 2.0)))
    }

    it("should code RDD[(Int, Tile)]") {
      val ds = Seq((1, byteArrayTile: Tile)).toDS
      assert(ds.toDF.as[(Int, Tile)].collect().head === (1, byteArrayTile))
    }

    it("should code RDD[Tile]") {
      val rdd = sc.makeRDD(Seq(byteArrayTile: Tile))
      val ds = rdd.toDF("tile")
      assert(ds.toDF.as[Tile].collect().head === byteArrayTile)
    }

    it("should code RDD[MultibandTile]") {
      val rdd = sc.makeRDD(Seq(multibandTile))
      val ds = rdd.toDS()
      assert(ds.toDF.as[MultibandTile].collect().head === multibandTile)
    }

    it("should code RDD[TileFeature]") {
      val thing = TileFeature(byteArrayTile: Tile, "meta")
      val ds = Seq(thing).toDS()
      assert(ds.toDF.as[TileFeature[Tile, String]].collect().head === thing)
    }

    it("should code RDD[Extent]") {
      val ds = Seq(extent).toDS()
      assert(ds.toDF.as[Extent].collect().head === extent)
      ds.select(flattenExtent($"value")).printSchema()
      ds.select(flattenExtent($"value")).show(false)
      ds.select(flattenExtent($"value") as "foo").select("foo.*").show(false)
    }

    it("should code RDD[ProjectedExtent]") {
      val ds = Seq(pe).toDS()
      assert(ds.toDF.as[ProjectedExtent].collect().head === pe)
    }
  }
}
