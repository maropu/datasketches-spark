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

import scala.util.control.NonFatal

import org.apache.datasketches.cpc.{CpcSketch, CpcUnion}
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

trait BaseDistinctCntSketchAggregate extends TypedImperativeAggregate[CpcSketch] {

  override def createAggregationBuffer(): CpcSketch = {
    new CpcSketch(SQLConf.get.distinctCntSketchLgK)
  }

  private lazy val updateFunc = children.head.dataType match {
    case LongType => (buffer: CpcSketch, v: Any) => {
      buffer.update(v.asInstanceOf[Long])
    }
    case StringType => (buffer: CpcSketch, v: Any) => {
      buffer.update(v.asInstanceOf[UTF8String].toString)
    }
    case t => throw new IllegalStateException(s"Unexpected data type ${t.catalogString}")
  }

  override def update(buffer: CpcSketch, input: InternalRow): CpcSketch = {
    val value = children.head.eval(input)
    if (value != null) {
      updateFunc(buffer, value)
    }
    buffer
  }

  override def merge(buffer: CpcSketch, other: CpcSketch)
      : CpcSketch = {
    val union = new CpcUnion(SQLConf.get.distinctCntSketchLgK)
    union.update(buffer)
    union.update(other)
    union.getResult
  }

  override def serialize(obj: CpcSketch): Array[Byte] = {
    obj.toByteArray()
  }

  override def deserialize(bytes: Array[Byte]): CpcSketch = {
    CpcSketch.heapify(Memory.wrap(bytes))
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated cardinality by CPC Sketch.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.1.1")
case class DistinctCntSketches(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends BaseDistinctCntSketchAggregate with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  override def prettyName: String = "approx_count_distinct_ex"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): DistinctCntSketches =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): DistinctCntSketches =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = child :: Nil

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(StringType, LongType) :: Nil

  override def eval(buffer: CpcSketch): Any = {
    buffer.getEstimate.toLong
  }
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
  since = "3.1.1")
case class SketchDistinctCnt(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends BaseDistinctCntSketchAggregate with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0)
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

  override def eval(buffer: CpcSketch): Any = {
    buffer.toByteArray()
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Combines (i.e. merges) multiple input sketch states into a single output state.
  """,
  group = "agg_funcs",
  since = "3.1.1")
case class CombineDistinctCntSketches(
    child: Expression,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[CpcSketch]
    with ImplicitCastInputTypes
    with Logging {

  def this(child: Expression) = {
    this(child, 0, 0)
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

  override def createAggregationBuffer(): CpcSketch = {
    new CpcSketch(SQLConf.get.distinctCntSketchLgK)
  }

  override def update(
      buffer: CpcSketch,
      input: InternalRow): CpcSketch = {
    try {
      val bytes = child.eval(input).asInstanceOf[Array[Byte]]
      val otherBuffer = CpcSketch.heapify(Memory.wrap(bytes))
      val union = new CpcUnion(SQLConf.get.distinctCntSketchLgK)
      union.update(buffer)
      union.update(otherBuffer)
      union.getResult
    } catch {
      case e @ NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${classOf[CpcSketch].getSimpleName} sketch data.")
        throw e
    }
  }

  override def merge(buffer: CpcSketch, other: CpcSketch): CpcSketch = {
    val union = new CpcUnion(SQLConf.get.distinctCntSketchLgK)
    union.update(buffer)
    union.update(other)
    union.getResult
  }

  override def eval(buffer: CpcSketch): Any = {
    buffer.toByteArray()
  }

  override def serialize(obj: CpcSketch): Array[Byte] = {
    obj.toByteArray()
  }

  override def deserialize(bytes: Array[Byte]): CpcSketch = {
    CpcSketch.heapify(Memory.wrap(bytes))
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Computes the approximate distinct count from an input sketch state.
  """,
  // group = "math_funcs",
  since = "3.1.1")
case class DistinctCntFromSketchState(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Logging {

  override def prettyName: String = "approx_count_distinct_estimate"

  override lazy val dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  @transient private[this] lazy val getOutputDistinctCnt = {
    (ar: Any) => try {
      val sketch = CpcSketch.heapify(Memory.wrap(ar.asInstanceOf[Array[Byte]]))
      sketch.getEstimate.toLong
    } catch {
      case NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${classOf[CpcSketch].getSimpleName} sketch data.")
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
