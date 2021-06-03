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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}
import org.apache.spark.sql.types._

class SketchExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("QuantileFromSketchState - KLL") {
    // SELECT approx_percentile_accumulate(c) FROM VALUES (0), (1), (2), (10) AS t(c);
    val bytes = Array[Byte](5, 1, 15, 0, -56, 0, 8, 0, 4, 0, 0, 0, 0, 0, 0, 0, -56,
      0, 1, 0, -60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 65, 0, 0, 32, 65, 0, 0, 0,
      64, 0, 0, -128, 63, 0, 0, 0, 0)
    checkEvaluation(
      QuantileFromSketchState(
        Literal(bytes, BinaryType),
        Literal(0.95, DoubleType),
        "KLL"),
      10.0)
  }

  test("QuantileFromSketchState - REQ") {
    // SELECT approx_percentile_accumulate(c) FROM VALUES (0), (1), (2), (10) AS t(c);
    val bytes = Array[Byte](2, 1, 17, 56, 12, 0, 1, 4, 0, 0, 0, 0, 0, 0, -128, 63,
      0, 0, 0, 64, 0, 0, 32, 65)
    checkEvaluation(
      QuantileFromSketchState(
        Literal(bytes, BinaryType),
        Literal(0.95, DoubleType),
        "REQ"),
      10.0)
  }

  test("FreqItemFromSketchState") {
    // SELECT approx_freqitems_accumulate(c) FROM VALUES ('a'), ('a'), ('b'), ('c'), ('a') AS t(c);
    val bytes = Array[Byte](4, 1, 10, 3, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3,
      0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 99, 1, 0, 0, 0, 98, 1, 0, 0, 0, 97)
    checkEvaluation(
      FreqItemFromSketchState(Literal(bytes, BinaryType)),
      Array(Row("a", 3L), Row("c", 1L), Row("b", 1L)))
  }

  test("DistinctCntFromSketchState") {
    // SELECT approx_count_distinct_accumulate(col)
    //   FROM VALUES ('a'), ('a'), ('b'), ('c'), ('a') AS tab(col);
    val bytes = Array[Byte](4, 1, 16, 11, 0, 10, -52, -109, 3, 0, 0, 0, 2, 0, 0, 0, -66,
      21, 24, 110, 3, 0, 0, 0)
    checkEvaluation(
      DistinctCntFromSketchState(Literal(bytes, BinaryType)),
      3L)
  }
}
