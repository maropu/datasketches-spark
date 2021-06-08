/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.Locale

import scala.language.implicitConversions

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, ConfigReader}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DistinctCntSketch, QuantileSketch}
import org.apache.spark.sql.internal.SQLConf

object DataSketchConf {

  /**
   * Implicitly injects the [[DataSketchConf]] into [[SQLConf]].
   */
  implicit def SQLConfToDataSketchConf(conf: SQLConf): DataSketchConf = new DataSketchConf(conf)

  private val sqlConfEntries = SQLConf.sqlConfEntries

  private def register(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    require(!sqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    sqlConfEntries.put(entry.key, entry)
  }

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate { entry =>
      SQLConf.staticConfKeys.add(entry.key)
      register(entry)
    }
  }

  val QUANTILE_SKETCH_IMPL = buildConf("spark.sql.dataSketches.quantiles.defaultImpl")
    .doc("A default implementation used in quantile estimation functions.")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(QuantileSketch.ImplType.values.map(_.toString))
    .createWithDefault(QuantileSketch.ImplType.REQ.toString)

  val QUANTILE_SKETCH_KLL_K = buildConf("spark.sql.dataSketches.quantiles.kll.k")
    .doc("Specifies the parameter `k` for the quantile sketch implementation " +
      "named `KLL`, `KllFloatsSketch`.")
    .intConf
    .checkValue(_ > 0, "The parameter `k` must be positive.")
    .createWithDefault(200)

  val QUANTILE_SKETCH_REQ_K = buildConf("spark.sql.dataSketches.quantiles.req.k")
    .doc("Specifies the parameter `k` for the quantile sketch implementation " +
      "named `REQ`, `ReqSketch`.")
    .intConf
    .checkValue(_ > 0, "The parameter `k` must be positive.")
    .createWithDefault(12)

  val QUANTILE_SKETCH_MERGEABLE_K = buildConf("spark.sql.dataSketches.quantiles.mergeable.k")
    .doc("Specifies the parameter `k` for the quantile sketch implementation " +
      "named `MERGEABLE`, `DoubleSketch`.")
    .intConf
    .checkValue(_ > 0, "The parameter `k` must be positive.")
    .createWithDefault(128)

  val FREQUENT_ITEM_SKETCH_MAX_MAP_SIZE = buildConf("spark.sql.dataSketches.freqItems.maxMapSize")
    .doc("Specifies the physical size of the internal hash map managed by this sketch and " +
      "must be a power of 2. The maximum capacity of this internal hash map is " +
      "0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are " +
      "functions of maxMapSize.")
    .intConf
    .checkValue(_ > 0, "The parameter `maxMapSize` must be a power of 2.")
    .createWithDefault(1024)

  val DISTINCT_COUNT_SKETCH_IMPL = buildConf("spark.sql.dataSketches.distinctCnt.defaultImpl")
    .doc("A default implementation used in distinct count estimation functions.")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(DistinctCntSketch.ImplType.values.map(_.toString))
    .createWithDefault(DistinctCntSketch.ImplType.CPC.toString)

  val DISTINCT_COUNT_SKETCH_CPC_LGK = buildConf("spark.sql.dataSketches.distinctCnt.cpc.lgK")
    .doc("Specifies the parameter `lgK` for the distinct count sketch implementation " +
      "named `CPC`, `CpcSketch`.")
    .intConf
    .checkValue(_ > 0, "The parameter `lgK` must be positive.")
    .createWithDefault(11)

  val DISTINCT_COUNT_SKETCH_HLL_LGK = buildConf("spark.sql.dataSketches.distinctCnt.hll.lgK")
    .doc("Specifies the parameter `lgK` for the distinct count sketch implementation " +
      "named `HLL`, `HllSketch`.")
    .intConf
    .checkValue(_ > 0, "The parameter `lgK` must be positive.")
    .createWithDefault(12)
}

class DataSketchConf(conf: SQLConf) {
  import DataSketchConf._

  private val reader = new ConfigReader(conf.settings)

  def quantileSketchImpl: String = getConf(QUANTILE_SKETCH_IMPL)

  def quantileSketchKInKll: Int = getConf(QUANTILE_SKETCH_KLL_K)

  def quantileSketchKInReq: Int = getConf(QUANTILE_SKETCH_REQ_K)

  def quantileSketchKInMergeable: Int = getConf(QUANTILE_SKETCH_MERGEABLE_K)

  def frequentItemSketchMaxMapSize: Int = getConf(FREQUENT_ITEM_SKETCH_MAX_MAP_SIZE)

  def distinctCntSketchImpl: String = getConf(DISTINCT_COUNT_SKETCH_IMPL)

  def distinctCntSketchCpcLgK: Int = getConf(DISTINCT_COUNT_SKETCH_CPC_LGK)

  def distinctCntSketchHllLgK: Int = getConf(DISTINCT_COUNT_SKETCH_HLL_LGK)

  /**
   * Return the value of configuration property for the given key. If the key is not set yet,
   * return `defaultValue` in [[ConfigEntry]].
   */
  private def getConf[T](entry: ConfigEntry[T]): T = {
    require(sqlConfEntries.get(entry.key) == entry || SQLConf.staticConfKeys.contains(entry.key),
      s"$entry is not registered")
    entry.readFrom(reader)
  }
}
