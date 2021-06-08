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

import org.apache.datasketches.cpc.{CpcSketch => jCpcSketch, CpcUnion}
import org.apache.datasketches.hll.{HllSketch => jHllSketch, Union => HllUnion}
import org.apache.datasketches.memory.Memory

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataSketchConf._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object DistinctCntSketch {

  object ImplType extends Enumeration {
    val CPC, HLL = Value
  }

  import ImplType._

  def apply(name: String): BaseDistinctCntSketchImpl = name.toUpperCase(Locale.ROOT) match {
    case tpe if tpe == CPC.toString =>
      val impl = new jCpcSketch(SQLConf.get.distinctCntSketchLgKInCpc)
      new CpcSketchImpl(impl)
    case tpe if tpe == HLL.toString =>
      val impl = new jHllSketch(SQLConf.get.distinctCntSketchLgKInHll)
      new HllSketchImpl(impl)
    case _ =>
      throw new IllegalStateException(s"Unknown distinct count sketch type: $name")
  }

  def apply(name: String, bytes: Array[Byte]): BaseDistinctCntSketchImpl = {
    name.toUpperCase(Locale.ROOT) match {
      case tpe if tpe == CPC.toString =>
        new CpcSketchImpl(jCpcSketch.heapify(Memory.wrap(bytes)))
      case tpe if tpe == HLL.toString =>
        new HllSketchImpl(jHllSketch.heapify(Memory.wrap(bytes)))
      case _ =>
        throw new IllegalStateException(s"Unknown distinct count sketch type: $name")
    }
  }
}

trait BaseDistinctCntSketchImpl {
  def impl: AnyRef
  def isEmpty: Boolean
  def update(v: Long): Unit
  def update(v: String): Unit
  def merge(other: BaseDistinctCntSketchImpl): Unit
  def getEstimate(): Long
  def serializeTo(): Array[Byte]
}

class CpcSketchImpl(var _impl: jCpcSketch) extends BaseDistinctCntSketchImpl {
  override def impl: AnyRef = _impl
  override def isEmpty: Boolean = _impl.isEmpty
  override def update(v: Long): Unit = _impl.update(v)
  override def update(v: String): Unit = _impl.update(v)
  override def merge(other: BaseDistinctCntSketchImpl): Unit = {
    val union = new CpcUnion(SQLConf.get.distinctCntSketchLgKInCpc)
    union.update(_impl)
    union.update(other.impl.asInstanceOf[jCpcSketch])
    _impl = union.getResult
  }
  def getEstimate(): Long = _impl.getEstimate().toLong
  override def serializeTo(): Array[Byte] = _impl.toByteArray
}

class HllSketchImpl(var _impl: jHllSketch) extends BaseDistinctCntSketchImpl {
  override def impl: AnyRef = _impl
  override def isEmpty: Boolean = _impl.isEmpty
  override def update(v: Long): Unit = _impl.update(v)
  override def update(v: String): Unit = _impl.update(v)
  override def merge(other: BaseDistinctCntSketchImpl): Unit = {
    val union = new HllUnion(SQLConf.get.distinctCntSketchLgKInHll)
    union.update(_impl)
    union.update(other.impl.asInstanceOf[jHllSketch])
    _impl = union.getResult
  }
  def getEstimate(): Long = _impl.getEstimate().toLong
  override def serializeTo(): Array[Byte] = _impl.toUpdatableByteArray
}

trait BaseDistinctCntSketchAggregate extends TypedImperativeAggregate[BaseDistinctCntSketchImpl] {

  def implName: String

  override def createAggregationBuffer(): BaseDistinctCntSketchImpl = {
    DistinctCntSketch(implName)
  }

  private lazy val updateFunc = children.head.dataType match {
    case LongType => (buffer: BaseDistinctCntSketchImpl, v: Any) => {
      buffer.update(v.asInstanceOf[Long])
    }
    case StringType => (buffer: BaseDistinctCntSketchImpl, v: Any) => {
      buffer.update(v.asInstanceOf[UTF8String].toString)
    }
    case t => throw new IllegalStateException(s"Unexpected data type ${t.catalogString}")
  }

  override def update(
      buffer: BaseDistinctCntSketchImpl,
      input: InternalRow): BaseDistinctCntSketchImpl = {
    val value = children.head.eval(input)
    if (value != null) {
      updateFunc(buffer, value)
    }
    buffer
  }

  override def merge(buffer: BaseDistinctCntSketchImpl, other: BaseDistinctCntSketchImpl)
      : BaseDistinctCntSketchImpl = {
    buffer.merge(other)
    buffer
  }

  override def serialize(obj: BaseDistinctCntSketchImpl): Array[Byte] = {
    obj.serializeTo()
  }

  override def deserialize(bytes: Array[Byte]): BaseDistinctCntSketchImpl = {
    DistinctCntSketch(implName, bytes)
  }
}

abstract class BaseDistinctCntSketch
  extends BaseDistinctCntSketchAggregate
  with ImplicitCastInputTypes {

  def child: Expression
  override def children: Seq[Expression] = child :: Nil
  // Returns null for empty inputs
  override def nullable: Boolean = true
  override lazy val dataType: DataType = LongType
  override def inputTypes: Seq[AbstractDataType] = TypeCollection(StringType, LongType) :: Nil
  override def eval(buffer: BaseDistinctCntSketchImpl): Any = {
    buffer.getEstimate
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated cardinality by the sketch algorithm.
      You can change the internal distinct count sketch algorithm via
      `spark.sql.dataSketches.distinctCnt.defaultImpl`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class DistinctCntSketch(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    implName: String) extends BaseDistinctCntSketch {

  def this(child: Expression) = {
    this(child, 0, 0, SQLConf.get.distinctCntSketchImpl)
  }

  override def prettyName: String = "approx_count_distinct_ex"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): DistinctCntSketch =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): DistinctCntSketch =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated cardinality by the CPC sketch algorithm.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class CpcSketch(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends BaseDistinctCntSketch {

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  override def implName: String = DistinctCntSketch.ImplType.CPC.toString
  override def prettyName: String = "approx_count_distinct_ex"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): CpcSketch =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): CpcSketch =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated cardinality by the HLL sketch algorithm.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class HllSketch(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends BaseDistinctCntSketch {

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  override def implName: String = DistinctCntSketch.ImplType.CPC.toString
  override def prettyName: String = "approx_count_distinct_ex"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): HllSketch =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllSketch =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Returns the internal representation of a distinct count sketch state
      at the end of aggregation.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES ('a'), ('a'), ('b'), ('c'), ('a') AS tab(col);
       04 01 10 0B 00 0A CC 93 03 00 00 00 02 00 00 00 BE 15 18 6E 03 00 00 00
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class SketchDistinctCnt(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    implName: String)
  extends BaseDistinctCntSketchAggregate with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0, SQLConf.get.distinctCntSketchImpl)
  }

  override def prettyName: String = "approx_freqitems_accumulate"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): SketchDistinctCnt =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): SketchDistinctCnt =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = child :: Nil

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = BinaryType

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def eval(buffer: BaseDistinctCntSketchImpl): Any = {
    buffer.serializeTo()
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Combines (i.e. merges) multiple input sketch states into a single output state.
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class CombineDistinctCntSketches(
    child: Expression,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int,
    implName: String)
  extends TypedImperativeAggregate[BaseDistinctCntSketchImpl]
    with ImplicitCastInputTypes
    with Logging {

  def this(child: Expression) = {
    this(child, 0, 0, SQLConf.get.distinctCntSketchImpl)
  }

  override def prettyName: String = "approx_count_distinct_combine"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int)
    : CombineDistinctCntSketches = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int)
    : CombineDistinctCntSketches = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def children: Seq[Expression] = {
    child :: Nil
  }

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = BinaryType

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def createAggregationBuffer(): BaseDistinctCntSketchImpl = {
    DistinctCntSketch(implName)
  }

  override def update(
      buffer: BaseDistinctCntSketchImpl,
      input: InternalRow): BaseDistinctCntSketchImpl = {
    try {
      val bytes = child.eval(input).asInstanceOf[Array[Byte]]
      buffer.merge(DistinctCntSketch(implName, bytes))
      buffer
    } catch {
      case e @ NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${buffer.impl.getClass.getSimpleName} sketch data.")
        throw e
    }
  }

  override def merge(
      buffer: BaseDistinctCntSketchImpl,
      other: BaseDistinctCntSketchImpl): BaseDistinctCntSketchImpl = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: BaseDistinctCntSketchImpl): Any = {
    buffer.serializeTo()
  }

  override def serialize(obj: BaseDistinctCntSketchImpl): Array[Byte] = {
    obj.serializeTo()
  }

  override def deserialize(bytes: Array[Byte]): BaseDistinctCntSketchImpl = {
    DistinctCntSketch(implName, bytes)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Computes the approximate distinct count from an input sketch state.
  """,
  // group = "math_funcs",
  since = "3.1.2")
case class DistinctCntFromSketchState(child: Expression, implName: String)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Logging {

  def this(child: Expression) = {
    this(child, SQLConf.get.distinctCntSketchImpl)
  }

  override def prettyName: String = "approx_count_distinct_estimate"

  override lazy val dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  @transient private[this] lazy val getOutputDistinctCnt = {
    (ar: Any) => try {
      val sketch = DistinctCntSketch(implName, ar.asInstanceOf[Array[Byte]])
      sketch.getEstimate
    } catch {
      case NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${DistinctCntSketch(implName).getClass.getSimpleName} sketch data.")
        null
    }
  }

  override def nullSafeEval(ar: Any): Any = getOutputDistinctCnt(ar)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val pf = ctx.addReferenceObj("getDistinctCnt", getOutputDistinctCnt,
      classOf[Any => Any].getCanonicalName)
    val distinctCnt = ctx.freshName("distinctCnt")
    nullSafeCodeGen(ctx, ev, ar => {
      s"""
         |Object $distinctCnt = $pf.apply($ar);
         |if ($distinctCnt != null) {
         |  ${ev.value} = ((${boxedType(dataType)}) $distinctCnt).${javaType(dataType)}Value();
         |} else {
         |  ${ev.isNull} = true;
         |}
       """.stripMargin
    })
  }
}
