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

package org.apache.spark.sql

import com.typesafe.scalalogging.LazyLogging

/**
 * Module providing support for using GeoTrellis native types in Spark SQL.
 *
 * @author sfitch 
 * @since 3/30/17
 */
object GTSQL extends LazyLogging {
  def init(sqlContext: SQLContext): Unit = {
    GTSQLTypes.register(sqlContext)
    GTSQLFunctions.register(sqlContext)
  }
}
