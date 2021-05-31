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
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}
import org.apache.spark.sql.types._

class QuantileSketchExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("FromQuantileSketch - KLL") {
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

  test("FromQuantileSketch - REQ") {
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
}
