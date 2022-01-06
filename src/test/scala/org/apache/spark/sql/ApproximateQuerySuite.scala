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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.aggregate.DataSketches
import org.apache.spark.sql.internal.DataSketchConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._

class ApproximateQuerySuite extends QueryTest with SharedSparkSession with SQLTestUtils {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", classOf[DataSketchExtensions].getName)
  }

  test("approximate percentile tests") {
    Seq("KLL", "REQ").foreach { impl =>
      withSQLConf(DataSketchConf.QUANTILE_SKETCH_IMPL.key -> impl) {
        val df1 = spark.sql(
          s"""
             |SELECT approx_percentile_ex(c, array(0.5, 0.4, 0.1))
             |  FROM VALUES (0), (1), (2), (null), (10) AS t(c);
           """.stripMargin)
        checkAnswer(df1, Row(Array(2, 1, 0)))

        val df2 = spark.sql(
          s"""
             |SELECT approx_percentile_ex(c, 0.5)
             |  FROM VALUES (0), (6), (7), (null), (9), (10) AS t(c);
           """.stripMargin)
        checkAnswer(df2, Row(7))
      }
    }
  }

  test("approx_percentile_ex should keep an input type in output") {
    val testTypes = Seq(("TINYINT", ByteType), ("INT", IntegerType), ("LONG", LongType),
      ("FLOAT", FloatType), ("DOUBLE", DoubleType), ("DECIMAL(10, 0)", DecimalType.IntDecimal))

    testTypes.foreach { case (inputType, expectedType) =>
      val df = spark.sql(
        s"""
           |SELECT approx_percentile_ex(CAST(c AS $inputType), 0.5)
           |  FROM VALUES (0), (null) AS t(c);
         """.stripMargin)
      assert(df.schema.head.dataType === expectedType)
      checkAnswer(df, Row(0))
    }
  }

  test("approx_percentile_ex - error handling") {
    val errMsg1 = intercept[AnalysisException] {
      spark.sql("SELECT approx_percentile_ex(c, p) FROM VALUES (0, 0.95) AS t(c, p)")
    }.getMessage()
    assert(errMsg1.contains("The percentage(s) must be a constant literal"))

    val errMsg2 = intercept[AnalysisException] {
      spark.sql("SELECT approx_percentile_ex(c, null) FROM VALUES (0) AS t(c)")
    }.getMessage()
    assert(errMsg2.contains("Percentage value must not be null"))

    Seq("-1.0", "array(0.1, -1.0)").foreach { p =>
      val errMsg3 = intercept[AnalysisException] {
        spark.sql(s"SELECT approx_percentile_ex(c, $p) FROM VALUES (0) AS t(c)")
      }.getMessage()
      assert(errMsg3.contains("Percentage(s) must be between 0.0 and 1.0"))
    }
  }

  test("approximate percentile tests - KLL/REQ/MERGEABLE") {
    Seq("approx_percentile_kll", "approx_percentile_req", "approx_percentile_mergeable")
        .foreach { f =>
      val df1 = spark.sql(
        s"""
           |SELECT $f(c, array(0.5, 0.4, 0.1))
           |  FROM VALUES (0), (1), (2), (null), (10) AS t(c);
         """.stripMargin)
      checkAnswer(df1, Row(Array(2, 1, 0)))

      val df2 = spark.sql(
        s"""
           |SELECT $f(c, 0.5)
           |  FROM VALUES (0), (6), (7), (null), (9), (10) AS t(c);
         """.stripMargin)
      checkAnswer(df2, Row(7))
    }
  }

  test("mergeable percentile summary tests") {
    import org.apache.spark.sql.functions._
    import testImplicits._

    withTempView("t") {
      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES
           |  (date("2021-01-01"), 1.0),
           |  (date("2021-01-01"), 1.0),
           |  (date("2021-01-01"), 2.0),
           |  (date("2021-01-02"), 3.0),
           |  (date("2021-01-02"), 2.0),
           |  (date("2021-01-02"), 1.0),
           |  (date("2021-01-02"), null),
           |  (date("2021-01-03"), 3.0),
           |  (date("2021-01-03"), 3.0),
           |  (date("2021-01-03"), 2.0),
           |  (date("2021-01-04"), 1.0)
           |AS t(date, v);
         """.stripMargin)

      val summaries = spark.table("t")
        .groupBy(window($"date", "1 day"))
        .agg(expr("approx_percentile_accumulate(v) AS summaries"))

      assert(summaries.schema.toDDL ===
        "`window` STRUCT<`start`: TIMESTAMP, `end`: TIMESTAMP> NOT NULL,`summaries` BINARY")
      checkAnswer(summaries.selectExpr("bit_length(summaries)"),
        Seq(Row(160), Row(160), Row(160), Row(96)))

      val merged = summaries
        .where("window.start >= '2021-01-01' AND window.end <= '2021-01-04'")
        .selectExpr("approx_percentile_combine(summaries) AS merged")

      val df1 = merged.selectExpr("approx_percentile_estimate(merged, 0.95)")
      checkAnswer(df1, Row(3.0))
      val df2 = merged.selectExpr("approx_percentile_estimate(merged, array(0.05, 0.50, 0.95))")
      checkAnswer(df2, Row(Array(1.0, 3.0, 3.0)))
      val df3 = merged.selectExpr("approx_pmf_estimate(merged, 2)")
      checkAnswer(df3, Row(Array(0.0, 1.0)))
    }
  }

  test("approx_percentile_estimate - error handling") {
    val errMsg1 = intercept[AnalysisException] {
      spark.sql(
        s"""
           |SELECT approx_percentile_estimate(s, p)
           |  FROM VALUES (binary('abc'), 0.95) AS t(s, p)
         """.stripMargin)
    }.getMessage()
    assert(errMsg1.contains("The percentage(s) must be a constant literal"))

    val errMsg2 = intercept[AnalysisException] {
      spark.sql(
        s"""
           |SELECT approx_percentile_estimate(s, null)
           |  FROM VALUES (binary('abc')) AS t(s)
         """.stripMargin)
    }.getMessage()
    assert(errMsg2.contains("Percentage value must not be null"))

    Seq("-1.0", "array(0.1, -1.0)").foreach { p =>
      val errMsg3 = intercept[AnalysisException] {
        spark.sql(
          s"""
             |SELECT approx_percentile_estimate(s, $p)
             |  FROM VALUES (binary('abc')) AS t(s)
           """.stripMargin)
      }.getMessage()
      assert(errMsg3.contains("Percentage(s) must be between 0.0 and 1.0"))
    }
  }

  test("approx_pmf_estimate - error handling") {
    val errMsg1 = intercept[AnalysisException] {
      spark.sql(
        s"""
           |SELECT approx_pmf_estimate(s, p)
           |  FROM VALUES (binary('abc'), 2) AS t(s, p)
         """.stripMargin)
    }.getMessage()
    assert(errMsg1.contains("The split number must be a constant literal"))

    Seq("null", "-1", "0", "1").foreach { numSplit =>
      val errMsg2 = intercept[AnalysisException] {
        spark.sql(
          s"""
             |SELECT approx_pmf_estimate(s, $numSplit)
             |  FROM VALUES (binary('abc')) AS t(s)
           """.stripMargin)
      }.getMessage()
      assert(errMsg2.contains("The split number must be greater than 1"))
    }
  }

  test("approximate frequent items tests") {
    val df = spark.sql(
      s"""
         |SELECT approx_freqitems(c)
         |  FROM VALUES ('a'), ('a'), ('b'), (null), ('c'), ('a') AS t(c);
       """.stripMargin)
    checkAnswer(df, Row(Array(Row("a", 3), Row("c", 1), Row("b", 1))))
  }

  test("approximate frequent items tests - integral types") {
    Seq("TINYINT", "SHORT", "INT", "LONG").foreach { inputType =>
      val df = spark.sql(
        s"""
           |SELECT approx_freqitems(CAST(c AS $inputType))
           |  FROM VALUES (1), (1), (2), (null), (3), (1) AS t(c);
         """.stripMargin)
      checkAnswer(df, Row(Array(Row(1, 3), Row(2, 1), Row(3, 1))))
    }
  }

  test("mergeable frequent items summary tests") {
    import org.apache.spark.sql.functions._
    import testImplicits._

    withTempView("t") {
      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES
           |  (date("2021-01-01"), 'a'),
           |  (date("2021-01-01"), 'a'),
           |  (date("2021-01-01"), 'a'),
           |  (date("2021-01-02"), 'b'),
           |  (date("2021-01-02"), 'a'),
           |  (date("2021-01-02"), 'b'),
           |  (date("2021-01-02"), null),
           |  (date("2021-01-03"), 'b'),
           |  (date("2021-01-03"), 'a'),
           |  (date("2021-01-03"), 'c'),
           |  (date("2021-01-04"), 'a')
           |AS t(date, v);
         """.stripMargin)

      val summaries = spark.table("t")
        .groupBy(window($"date", "1 day"))
        .agg(expr("approx_freqitems_accumulate(v) AS summaries"))

      assert(summaries.schema.toDDL ===
        "`window` STRUCT<`start`: TIMESTAMP, `end`: TIMESTAMP> NOT NULL,`summaries` BINARY")
      checkAnswer(summaries.selectExpr("bit_length(summaries)"),
        Seq(Row(360), Row(360), Row(464), Row(568)))

      val merged = summaries
        .where("window.start >= '2021-01-01' AND window.end <= '2021-01-04'")
        .selectExpr("approx_freqitems_combine(summaries) AS merged")

      val df = merged.selectExpr("approx_freqitems_estimate(merged)")
      checkAnswer(df, Row(Array(Row("b", 3), Row("a", 2), Row("c", 1))))
    }
  }

  test("approximate distinct count tests") {
    Seq("approx_count_distinct_ex",
      "approx_count_distinct_cpc",
      "approx_count_distinct_hll").foreach { f =>
      val df1 = spark.sql(
        s"SELECT $f(c) FROM VALUES ('a'), ('a'), ('b'), (null), ('b'), ('c') AS t(c);")
      checkAnswer(df1, Row(3L))

      Seq("TINYINT", "SHORT", "INT", "LONG", "STRING").foreach { inputType =>
        val df2 = spark.sql(
          s"""
             |SELECT $f(CAST(c AS $inputType))
             |  FROM VALUES (1), (1), (2), (null), (2), (3) AS t(c);
           """.stripMargin)
        checkAnswer(df2, Row(3L))
      }
    }
  }

  test("mergeable distinct count summary tests") {
    import org.apache.spark.sql.functions._
    import testImplicits._

    withTempView("t") {
      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES
           |  (date("2021-01-01"), 'a'),
           |  (date("2021-01-01"), 'a'),
           |  (date("2021-01-01"), 'a'),
           |  (date("2021-01-02"), 'b'),
           |  (date("2021-01-02"), 'a'),
           |  (date("2021-01-02"), 'b'),
           |  (date("2021-01-02"), null),
           |  (date("2021-01-03"), 'b'),
           |  (date("2021-01-03"), 'a'),
           |  (date("2021-01-03"), 'c'),
           |  (date("2021-01-04"), 'a')
           |AS t(date, v);
         """.stripMargin)

      val summaries = spark.table("t")
        .groupBy(window($"date", "1 day"))
        .agg(expr("approx_count_distinct_accumulate(v) AS summaries"))

      assert(summaries.schema.toDDL ===
        "`window` STRUCT<`start`: TIMESTAMP, `end`: TIMESTAMP> NOT NULL,`summaries` BINARY")
      checkAnswer(summaries.selectExpr("bit_length(summaries)"),
        Seq(Row(160), Row(160), Row(160), Row(192)))

      val merged = summaries
        .where("window.start >= '2021-01-01' AND window.end <= '2021-01-04'")
        .selectExpr("approx_count_distinct_combine(summaries) AS merged")

      val df = merged.selectExpr("approx_count_distinct_estimate(merged)")
      checkAnswer(df, Row(3L))
    }
  }

  test("approx_percentile_estimate ignores an input type in output") {
    // TODO: Fix this test failure:
    //  org.apache.spark.sql.AnalysisException: Undefined function: 'approx_percentile_accumulate'.
    //    This function is neither a registered temporary function nor a permanent function
    //    registered in the database 'default'.; line 1 pos 7
    DataSketches.install()

    Seq("TINYINT", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DECIMAL(10, 0)")
      .foreach { inputType =>
        withTempView("t") {
          spark.sql(
            s"""
               |CREATE TEMPORARY VIEW t AS
               |  SELECT approx_percentile_accumulate(CAST(c AS $inputType)) summaries
               |    FROM VALUES (0), (1), (2), (null), (10) AS t(c);
             """.stripMargin)

          val df = spark.sql(
            s"""
               |SELECT approx_percentile_estimate(summaries, 0.5) FROM t;
             """.stripMargin)
          assert(df.schema.head.dataType === DoubleType)
          checkAnswer(df, Row(2.0))
        }
      }
  }
}
