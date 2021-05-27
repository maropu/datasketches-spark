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

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.datasketches.kll.{KllFloatsSketch => jKllFloatsSketch}
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.req.{ReqSketch => jReqSketch}

import org.apache.spark.sql.DataSketchConf._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object QuantileSketch {

  object ImplType extends Enumeration {
    val KLL, REQ = Value
  }

  import ImplType._

  def apply(name: String): BaseQuantileSketchImpl = name.toUpperCase(Locale.ROOT) match {
    case tpe if tpe == KLL.toString =>
      val k = SQLConf.get.quantileSketchKInKLL
      val impl = new jKllFloatsSketch(k)
      new KllFloatsSketchImpl(impl)
    case tpe if tpe == REQ.toString =>
      val k = SQLConf.get.quantileSketchKInREQ
      val impl = jReqSketch.builder().setK(k).build()
      new ReqSketchImpl(impl)
    case _ => throw new IllegalStateException(s"Unknown percentile sketch type: $name")
  }

  def apply(name: String, bytes: Array[Byte]): BaseQuantileSketchImpl = {
    name.toUpperCase(Locale.ROOT) match {
      case tpe if tpe == KLL.toString =>
        val impl = jKllFloatsSketch.heapify(Memory.wrap(bytes))
        new KllFloatsSketchImpl(impl)
      case tpe if tpe == REQ.toString =>
        val impl = jReqSketch.heapify(Memory.wrap(bytes))
        new ReqSketchImpl(impl)
      case _ => throw new IllegalStateException(s"Unknown percentile sketch type: $name")
    }
  }
}

trait BaseQuantileSketchImpl {
  def name: String
  def impl: AnyRef
  def isEmpty: Boolean
  def update(v: Float): Unit
  def merge(other: BaseQuantileSketchImpl): Unit
  def getQuantiles(fractions: Array[Double]): Array[Float]
  def serializeTo(): Array[Byte]
}

class KllFloatsSketchImpl(_impl: jKllFloatsSketch) extends BaseQuantileSketchImpl {
  override def name: String = "kll"
  override def impl: AnyRef = _impl
  override def isEmpty: Boolean = _impl.isEmpty
  override def update(v: Float): Unit = _impl.update(v)
  override def merge(other: BaseQuantileSketchImpl): Unit =
    _impl.merge(other.impl.asInstanceOf[jKllFloatsSketch])
  override def getQuantiles(fractions: Array[Double]): Array[Float] =
    _impl.getQuantiles(fractions)
  override def serializeTo(): Array[Byte] = _impl.toByteArray
}

class ReqSketchImpl(_impl: jReqSketch) extends BaseQuantileSketchImpl {
  override def name: String = "req"
  override def impl: AnyRef = _impl
  override def isEmpty: Boolean = _impl.isEmpty
  override def update(v: Float): Unit = _impl.update(v)
  override def merge(other: BaseQuantileSketchImpl): Unit =
    _impl.merge(other.impl.asInstanceOf[jReqSketch])
  override def getQuantiles(fractions: Array[Double]): Array[Float] =
    _impl.getQuantiles(fractions)
  override def serializeTo(): Array[Byte] = _impl.toByteArray
}

abstract class BaseQuantileSketch
  extends TypedImperativeAggregate[BaseQuantileSketchImpl]
  with ImplicitCastInputTypes {

  def implName: String
  def child: Expression
  def percentageExpression: Expression

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  @transient
  private lazy val returnPercentileArray = percentageExpression.dataType.isInstanceOf[ArrayType]

  @transient
  private lazy val percentages = percentageExpression.eval() match {
    case null => null
    case num: Double => Array(num)
    case arrayData: ArrayData => arrayData.toDoubleArray()
  }

  override def children: Seq[Expression] = {
    child :: percentageExpression :: Nil
  }

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = percentageExpression.dataType match {
    case _: ArrayType => ArrayType(DoubleType, false)
    case _ => DoubleType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    val percentageExpType = percentageExpression.dataType match {
      case _: ArrayType => ArrayType(DoubleType, false)
      case _ => DoubleType
    }
    Seq(NumericType, percentageExpType)
  }

  // Check the inputTypes are valid, and the percentageExpression satisfies:
  // 1. percentageExpression must be foldable;
  // 2. percentages(s) must be in the range [0.0, 1.0].
  override def checkInputDataTypes(): TypeCheckResult = {
    // Validate the inputTypes
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!percentageExpression.foldable) {
      // percentageExpression must be foldable
      TypeCheckFailure("The percentage(s) must be a constant literal, " +
        s"but got $percentageExpression")
    } else if (percentages == null) {
      TypeCheckFailure("Percentage value must not be null")
    } else if (percentages.exists(percentage => percentage < 0.0 || percentage > 1.0)) {
      // percentages(s) must be in the range [0.0, 1.0]
      TypeCheckFailure("Percentage(s) must be between 0.0 and 1.0, " +
        s"but got $percentageExpression")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): BaseQuantileSketchImpl = {
    QuantileSketch(implName)
  }

  override def update(
      buffer: BaseQuantileSketchImpl,
      input: InternalRow): BaseQuantileSketchImpl = {
    val value = child.eval(input).asInstanceOf[AnyRef]
    // Ignore empty rows, for example: percentile_approx(null)
    if (value != null) {
      // Convert the value to a float value
      val floatValue = child.dataType match {
        case DateType => value.asInstanceOf[Int].toFloat
        case TimestampType => value.asInstanceOf[Long].toFloat
        case n: NumericType => n.numeric.toFloat(value.asInstanceOf[n.InternalType])
        case other: DataType =>
          throw new UnsupportedOperationException(
            s"Unexpected data type ${other.catalogString}")
      }
      buffer.update(floatValue)
    }
    buffer
  }

  override def merge(buffer: BaseQuantileSketchImpl, other: BaseQuantileSketchImpl)
      : BaseQuantileSketchImpl = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: BaseQuantileSketchImpl): Any = {
    generateOutput(getPercentiles(buffer))
  }

  private def getPercentiles(buffer: BaseQuantileSketchImpl): Seq[Double] = {
    if (!buffer.isEmpty) {
      buffer.getQuantiles(percentages).map(_.toDouble).toSeq
    } else {
      Nil
    }
  }

  private def generateOutput(results: Seq[Double]): Any = {
    if (results.isEmpty) {
      null
    } else if (returnPercentileArray) {
      new GenericArrayData(results)
    } else {
      results.head
    }
  }

  override def serialize(obj: BaseQuantileSketchImpl): Array[Byte] = {
    obj.serializeTo()
  }

  override def deserialize(bytes: Array[Byte]): BaseQuantileSketchImpl = {
    QuantileSketch(implName, bytes)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col, percentage) - Returns the approximate `percentile` of the numeric column `col`.
      The value of percentage must be between 0.0 and 1.0. When `percentage`
      is an array, each value of the percentage array must be between 0.0 and 1.0.
      In this case, returns the approximate percentile array of column `col`
      at the given percentage array. You can change the internal percentile sketch
      algorithm via `spark.sql.dataSketches.quantiles.defaultImpl`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, array(0.5, 0.4, 0.1)) FROM VALUES (0), (1), (2), (10) AS tab(col);
       [2.0,1.0,0.0]
      > SELECT _FUNC_(col, 0.5) FROM VALUES (0), (6), (7), (9), (10) AS tab(col);
       7.0
  """,
  group = "agg_funcs",
  since = "3.1.1")
case class QuantileSketch(
    child: Expression,
    percentageExpression: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    implName: String = SQLConf.get.quantileSketchType) extends BaseQuantileSketch {

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, 0, 0)
  }

  override def prettyName: String = "approx_percentile_ex"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): QuantileSketch =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): QuantileSketch =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

@ExpressionDescription(
  usage = """
    _FUNC_(col, percentage) - Returns the approximate `percentile` of the numeric
      column `col` by using a very compact quantile sketch with lazy compaction scheme and
      nearly optimal accuracy per bit. The value of percentage must be between 0.0 and 1.0.
      When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
      In this case, returns the approximate percentile array of column `col`
      at the given percentage array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, array(0.5, 0.4, 0.1)) FROM VALUES (0), (1), (2), (10) AS tab(col);
       [2.0,1.0,0.0]
      > SELECT _FUNC_(col, 0.5) FROM VALUES (0), (6), (7), (9), (10) AS tab(col);
       7.0
  """,
  group = "agg_funcs",
  since = "3.1.1")
case class KllFloatsSketch(
    child: Expression,
    percentageExpression: Expression,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int) extends BaseQuantileSketch {

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, 0, 0)
  }

  override def implName: String = QuantileSketch.ImplType.KLL.toString
  override def prettyName: String = "approx_percentile_kll"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllFloatsSketch =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllFloatsSketch =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

@ExpressionDescription(
  usage = """
    _FUNC_(col, percentage) - Returns the approximate `percentile` of the numeric
      column `col` by using a relative error quantile (REQ) sketch that provides extremely
      high accuracy at a chosen end of the rank domain. The value of percentage must be
      between 0.0 and 1.0. When `percentage` is an array, each value of the percentage array
      must be between 0.0 and 1.0. In this case, returns the approximate percentile array
      of column `col` at the given percentage array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, array(0.5, 0.4, 0.1)) FROM VALUES (0), (1), (2), (10) AS tab(col);
       [2.0,1.0,0.0]
      > SELECT _FUNC_(col, 0.5) FROM VALUES (0), (6), (7), (9), (10) AS tab(col);
       7.0
  """,
  group = "agg_funcs",
  since = "3.1.1")
case class ReqSketch(
    child: Expression,
    percentageExpression: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends BaseQuantileSketch {

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, 0, 0)
  }

  override def implName: String = QuantileSketch.ImplType.REQ.toString
  override def prettyName: String = "approx_percentile_req"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ReqSketch =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ReqSketch =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(col) - Returns the internal representation of a percentile sketch state
      at the end of aggregation. You can change the percentile sketch algorithm
      via `spark.sql.dataSketches.quantiles.defaultImpl`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, array(0.5, 0.4, 0.1)) FROM VALUES (0), (1), (2), (10) AS tab(col);
       [5, 1, 15, 0, -56, 0, 8, 0, 4, 0, 0, 0, 0, 0, 0, 0, -56, 0, 1, 0, -60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 65, 0, 0, 32, 65, 0, 0, 0, 64, 0, 0, -128, 63, 0, 0, 0, 0]
  """,
  group = "agg_funcs",
  since = "3.1.1")
// scalastyle:on line.size.limit
case class SketchQuantile(
    child: Expression,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int,
    implName: String)
  extends TypedImperativeAggregate[BaseQuantileSketchImpl]
  with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0, SQLConf.get.quantileSketchType)
  }

  override def prettyName: String = "approx_percentile_accumulate"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): SketchQuantile =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): SketchQuantile =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override lazy val dataType: DataType = ArrayType(ByteType)

  override def children: Seq[Expression] = child :: Nil

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def createAggregationBuffer(): BaseQuantileSketchImpl = {
    QuantileSketch(implName)
  }

  override def eval(buffer: BaseQuantileSketchImpl): Any = {
    new GenericArrayData(buffer.serializeTo())
  }

  override def update(
      buffer: BaseQuantileSketchImpl,
      input: InternalRow): BaseQuantileSketchImpl = {
    val value = child.eval(input).asInstanceOf[AnyRef]
    // Ignore empty rows, for example: percentile_approx(null)
    if (value != null) {
      // Convert the value to a float value
      val floatValue = child.dataType match {
        case DateType => value.asInstanceOf[Int].toFloat
        case TimestampType => value.asInstanceOf[Long].toFloat
        case n: NumericType => n.numeric.toFloat(value.asInstanceOf[n.InternalType])
        case other: DataType =>
          throw new UnsupportedOperationException(
            s"Unexpected data type ${other.catalogString}")
      }
      buffer.update(floatValue)
    }
    buffer
  }

  override def merge(buffer: BaseQuantileSketchImpl, other: BaseQuantileSketchImpl)
      : BaseQuantileSketchImpl = {
    buffer.merge(other)
    buffer
  }

  override def serialize(obj: BaseQuantileSketchImpl): Array[Byte] = {
    obj.serializeTo()
  }

  override def deserialize(bytes: Array[Byte]): BaseQuantileSketchImpl = {
    QuantileSketch(implName, bytes)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col, percentage) - Combines (i.e. merges) multiple input sketch states into
      a single output state. Each state should be the one that the percentile sketch algorithm
      specified by `spark.sql.dataSketches.quantiles.defaultImpl` generates.
  """,
  group = "agg_funcs",
  since = "3.1.1")
case class CombineQuantileSketches(
    child: Expression,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int,
    implName: String)
  extends TypedImperativeAggregate[BaseQuantileSketchImpl]
  with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0, SQLConf.get.quantileSketchType)
  }

  override def prettyName: String = "approx_percentile_combine"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int)
    : CombineQuantileSketches = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int)
    : CombineQuantileSketches = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def children: Seq[Expression] = {
    child :: Nil
  }

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = ArrayType(ByteType)

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType(ByteType))

  override def createAggregationBuffer(): BaseQuantileSketchImpl = {
    QuantileSketch(implName)
  }

  private lazy val convert = CatalystTypeConverters.createToScalaConverter(child.dataType)

  override def update(
      buffer: BaseQuantileSketchImpl,
      input: InternalRow): BaseQuantileSketchImpl = {
    try {
      val bytes = convert(child.eval(input)).asInstanceOf[Seq[Byte]].toArray
      buffer.merge(QuantileSketch(implName, bytes))
    } catch {
      case NonFatal(_) => // Do nothing
    }
    buffer
  }

  override def merge(buffer: BaseQuantileSketchImpl, other: BaseQuantileSketchImpl)
      : BaseQuantileSketchImpl = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: BaseQuantileSketchImpl): Any = {
    new GenericArrayData(buffer.serializeTo())
  }

  override def serialize(obj: BaseQuantileSketchImpl): Array[Byte] = {
    obj.serializeTo()
  }

  override def deserialize(bytes: Array[Byte]): BaseQuantileSketchImpl = {
    QuantileSketch(implName, bytes)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col, percentage) - Computes an approximate `percentile` from an input sketch states.
      The value of percentage must be between 0.0 and 1.0. When `percentage` is an array,
      each value of the percentage array must be between 0.0 and 1.0. In this case,
      returns the approximate percentile array of column `col` at the given percentage array.
      The input state should be the one that the percentile sketch algorithm specified by
      `spark.sql.dataSketches.quantiles.defaultImpl` generates.
  """,
  // group = "math_funcs",
  since = "3.1.1")
case class FromQuantileSketch(
    child: Expression,
    percentageExpression: Expression,
    implName: String)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, SQLConf.get.quantileSketchType)
  }

  override def prettyName: String = "approx_percentile_estimate"
  override def left: Expression = child
  override def right: Expression = percentageExpression

  override lazy val dataType: DataType = percentageExpression.dataType match {
    case _: ArrayType => ArrayType(DoubleType, false)
    case _ => DoubleType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    val percentageExpType = percentageExpression.dataType match {
      case _: ArrayType => ArrayType(DoubleType, false)
      case _ => DoubleType
    }
    Seq(ArrayType(ByteType), percentageExpType)
  }

  // Check the inputTypes are valid, and the percentageExpression satisfies:
  // 1. percentageExpression must be foldable;
  // 2. percentages(s) must be in the range [0.0, 1.0].
  override def checkInputDataTypes(): TypeCheckResult = {
    // Validate the inputTypes
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!percentageExpression.foldable) {
      // percentageExpression must be foldable
      TypeCheckFailure("The percentage(s) must be a constant literal, " +
        s"but got $percentageExpression")
    } else if (percentages == null) {
      TypeCheckFailure("Percentage value must not be null")
    } else if (percentages.exists(percentage => percentage < 0.0 || percentage > 1.0)) {
      // percentages(s) must be in the range [0.0, 1.0]
      TypeCheckFailure("Percentage(s) must be between 0.0 and 1.0, " +
        s"but got $percentageExpression")
    } else {
      TypeCheckSuccess
    }
  }

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  @transient
  private lazy val returnPercentileArray = percentageExpression.dataType.isInstanceOf[ArrayType]

  @transient
  private lazy val percentages = percentageExpression.eval() match {
    case null => null
    case num: Double => Array(num)
    case arrayData: ArrayData => arrayData.toDoubleArray()
  }

  override def nullable: Boolean = true

  private def getPercentiles(buffer: BaseQuantileSketchImpl): Seq[Double] = {
    if (!buffer.isEmpty) {
      buffer.getQuantiles(percentages).map(_.toDouble).toSeq
    } else {
      Nil
    }
  }

  private def generateOutput(results: Seq[Double]): Any = {
    if (results.isEmpty) {
      null
    } else if (returnPercentileArray) {
      new GenericArrayData(results)
    } else {
      results.head
    }
  }

  @transient private[this] lazy val getOutputPercentiles = (ar: Any) => {
    try {
      val bytes = ar.asInstanceOf[ArrayData].toByteArray()
      val sketch = QuantileSketch(implName, bytes)
      generateOutput(getPercentiles(sketch))
    } catch {
      case NonFatal(_) => null
    }
  }

  override def nullSafeEval(ar: Any, percentages: Any): Any = getOutputPercentiles(ar)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val pf = ctx.addReferenceObj("getPrcentiles", getOutputPercentiles,
      classOf[Any => Any].getCanonicalName)
    nullSafeCodeGen(ctx, ev, (ar, _) => {
      if (!returnPercentileArray) {
        s"${ev.value} = ((${boxedType(dataType)})$pf.apply($ar)).${javaType(dataType)}Value();"
      } else {
        s"${ev.value} = (${javaType(dataType)})$pf.apply($ar);"
      }
    })
  }
}
