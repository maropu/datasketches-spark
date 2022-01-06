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

package org.apache.spark.sql.catalyst.expressions.aggregate

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, FunctionRegistryBase}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._

object DataSketches {
  import FunctionRegistoryShim._

  private var _installed = false

  val sketchExprs = Seq(
    // Quantile sketches
    expression[QuantileSketch]("approx_percentile_ex"),
    expression[KllFloatsSketch]("approx_percentile_kll"),
    expression[ReqSketch]("approx_percentile_req"),
    expression[MergeableSketch]("approx_percentile_mergeable"),
    expression[SketchQuantile]("approx_percentile_accumulate"),
    expression[CombineQuantileSketches]("approx_percentile_combine"),
    expression[QuantileFromSketchState]("approx_percentile_estimate"),
    expression[PmfFromSketchState]("approx_pmf_estimate"),

    // Frequent item sketches
    expression[FreqItemSketches]("approx_freqitems"),
    expression[SketchFreqItems]("approx_freqitems_accumulate"),
    expression[CombineFreqItemSketches]("approx_freqitems_combine"),
    expression[FreqItemFromSketchState]("approx_freqitems_estimate"),

    // Distinct count sketches
    expression[DistinctCntSketch]("approx_count_distinct_ex"),
    expression[CpcSketch]("approx_count_distinct_cpc"),
    expression[HllSketch]("approx_count_distinct_hll"),
    expression[SketchDistinctCnt]("approx_count_distinct_accumulate"),
    expression[CombineDistinctCntSketches]("approx_count_distinct_combine"),
    expression[DistinctCntFromSketchState]("approx_count_distinct_estimate")
  )

  def install(): Unit = synchronized {
    if (!_installed) {
      sketchExprs.foreach { case (name, (info, builder)) =>
        FunctionRegistry.builtin.registerFunction(FunctionIdentifier(name), info, builder)
      }
      _installed = true
    }
  }
}

/**
 * The codes in this file were copied from
 * `org.apache.spark.sql.catalyst.analysis.FunctionRegistory`.
 *
 * NOTE: The upcoming Spark v3.2.0 will have a functionality to automatically
 * load additional functions when initializing `SparkSession`
 * (For more details, see SPARK-35380).
 */
object FunctionRegistoryShim {

  /**
   * Create a SQL function builder and corresponding `ExpressionInfo`.
   * @param name The function name.
   * @param setAlias The alias name used in SQL representation string.
   * @param since The Spark version since the function is added.
   * @tparam T The actual expression class.
   * @return (function name, (expression information, function builder))
   */
  private[aggregate] def expression[T <: Expression : ClassTag](
      name: String,
      setAlias: Boolean = false,
      since: Option[String] = None): (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)
    val newBuilder = (expressions: Seq[Expression]) => {
      val expr = builder(expressions)
      if (setAlias) expr.setTagValue(FunctionRegistry.FUNC_ALIAS, name)
      expr
    }
    (name, (expressionInfo, newBuilder))
  }
}
