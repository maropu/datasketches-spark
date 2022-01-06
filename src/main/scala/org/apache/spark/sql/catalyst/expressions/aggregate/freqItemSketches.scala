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

import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch, LongsSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.DataSketchConf._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object FreqSketch {

  def apply(dataType: DataType): BaseFreqSketchImpl = {
    val maxMapSize = SQLConf.get.frequentItemSketchMaxMapSize
    dataType match {
      case StringType => new StringFreqSketchImpl(new ItemsSketch[String](maxMapSize))
      case LongType => new LongFreqSketchImpl(new LongsSketch(maxMapSize))
      case t => throw new IllegalStateException(s"Unknown input type: $t")
    }
  }

  def apply(bytes: Array[Byte], dataType: DataType): BaseFreqSketchImpl = dataType match {
    case StringType =>
      val impl = ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe())
      new StringFreqSketchImpl(impl)
    case LongType =>
      val impl = LongsSketch.getInstance(Memory.wrap(bytes))
      new LongFreqSketchImpl(impl)
    case t =>
      throw new IllegalStateException(s"Unknown input type: $t")
  }
}

trait BaseFreqSketchImpl {
  def impl: AnyRef
  def isEmpty: Boolean
  def update(v: Any): Unit
  def merge(other: BaseFreqSketchImpl): Unit
  def getFrequentItems(): Array[(Any, Long)]
  def serializeTo(): Array[Byte]
}

class StringFreqSketchImpl(_impl: ItemsSketch[String]) extends BaseFreqSketchImpl {
  override def impl: AnyRef = _impl
  override def isEmpty: Boolean = _impl.isEmpty
  override def update(v: Any): Unit = _impl.update(v.asInstanceOf[UTF8String].toString)
  override def merge(other: BaseFreqSketchImpl): Unit =
    _impl.merge(other.impl.asInstanceOf[ItemsSketch[String]])
  def getFrequentItems(): Array[(Any, Long)] = {
    _impl.getFrequentItems(ErrorType.NO_FALSE_POSITIVES).map { i =>
      (i.getItem, i.getEstimate)
    }
  }
  override def serializeTo(): Array[Byte] = {
    _impl.toByteArray(new ArrayOfStringsSerDe())
  }
}

class LongFreqSketchImpl(_impl: LongsSketch) extends BaseFreqSketchImpl {
  override def impl: AnyRef = _impl
  override def isEmpty: Boolean = _impl.isEmpty
  override def update(v: Any): Unit = _impl.update(v.asInstanceOf[Long])
  override def merge(other: BaseFreqSketchImpl): Unit =
    _impl.merge(other.impl.asInstanceOf[LongsSketch])
  def getFrequentItems(): Array[(Any, Long)] = {
    _impl.getFrequentItems(ErrorType.NO_FALSE_POSITIVES).map { i =>
      (i.getItem, i.getEstimate)
    }
  }
  override def serializeTo(): Array[Byte] = {
    _impl.toByteArray()
  }
}

trait BaseFreqItemSketchAggregate extends TypedImperativeAggregate[BaseFreqSketchImpl] {

  override def createAggregationBuffer(): BaseFreqSketchImpl = {
    FreqSketch(children.head.dataType)
  }

  override def update(
      buffer: BaseFreqSketchImpl,
      input: InternalRow): BaseFreqSketchImpl = {
    val value = children.head.eval(input)
    if (value != null) {
      buffer.update(value)
    }
    buffer
  }

  override def merge(buffer: BaseFreqSketchImpl, other: BaseFreqSketchImpl)
      : BaseFreqSketchImpl = {
    buffer.merge(other)
    buffer
  }

  override def serialize(obj: BaseFreqSketchImpl): Array[Byte] = {
    obj.serializeTo()
  }

  override def deserialize(bytes: Array[Byte]): BaseFreqSketchImpl = {
    FreqSketch(bytes, children.head.dataType)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Returns the approximate "heaviest" or "most frequently occurring"
      items of the string column `col`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES ('a'), ('a'), ('b'), ('c'), ('a') AS tab(col);
       [{a, 3}, {c, 1}, {b, 1}]
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class FreqItemSketches(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends BaseFreqItemSketchAggregate with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  override def prettyName: String = "approx_freqitems"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): FreqItemSketches =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): FreqItemSketches =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = child :: Nil

  private lazy val inputType: DataType = child.dataType

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = {
    ArrayType(StructType(Seq(StructField("item", inputType), StructField("estimated", LongType))))
  }

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(LongType, StringType) :: Nil

  private lazy val generateOutput = inputType match {
    case StringType => (v: Any) => UTF8String.fromString(v.toString)
    case ByteType => (v: Any) => v.asInstanceOf[Long].toByte
    case ShortType => (v: Any) => v.asInstanceOf[Long].toShort
    case IntegerType => (v: Any) => v.asInstanceOf[Long].toInt
    case LongType => (v: Any) => v
    case t => throw new IllegalStateException(s"Unknown input type: $t")
  }

  override def eval(buffer: BaseFreqSketchImpl): Any = {
    val freqItems = buffer.getFrequentItems().map { case (item, estimate) =>
      InternalRow(generateOutput(item), estimate)
    }
    new GenericArrayData(freqItems)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Returns the internal representation of a frequent item sketch state
      at the end of aggregation.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES ('a'), ('a'), ('b'), ('c'), ('a') AS tab(col);
       04 01 0A 03 03 00 00 00 03 00 00 00 00 00 00 00 05 00 ... 61
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class SketchFreqItems(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends BaseFreqItemSketchAggregate with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  override def prettyName: String = "approx_freqitems_accumulate"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): SketchFreqItems =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): SketchFreqItems =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def createAggregationBuffer(): BaseFreqSketchImpl = {
    FreqSketch(StringType)
  }

  override def children: Seq[Expression] = child :: Nil

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = BinaryType

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def eval(buffer: BaseFreqSketchImpl): Any = {
    buffer.serializeTo()
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Combines (i.e. merges) multiple input sketch states into a single output state.
  """,
  group = "agg_funcs",
  since = "3.1.2")
case class CombineFreqItemSketches(
    child: Expression,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[BaseFreqSketchImpl]
  with ImplicitCastInputTypes
  with Logging {

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  override def prettyName: String = "approx_freqitems_combine"
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int)
    : CombineFreqItemSketches = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int)
    : CombineFreqItemSketches = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def children: Seq[Expression] = {
    child :: Nil
  }

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = BinaryType

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def createAggregationBuffer(): BaseFreqSketchImpl = {
    FreqSketch(StringType)
  }

  override def update(
      buffer: BaseFreqSketchImpl,
      input: InternalRow): BaseFreqSketchImpl = {
    try {
      val bytes = child.eval(input).asInstanceOf[Array[Byte]]
      buffer.merge(FreqSketch(bytes, StringType))
    } catch {
      case e @ NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${buffer.impl.getClass.getSimpleName} sketch data.")
        throw e
    }
    buffer
  }

  override def merge(buffer: BaseFreqSketchImpl, other: BaseFreqSketchImpl)
      : BaseFreqSketchImpl = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: BaseFreqSketchImpl): Any = {
    buffer.serializeTo()
  }

  override def serialize(obj: BaseFreqSketchImpl): Array[Byte] = {
    obj.serializeTo()
  }

  override def deserialize(bytes: Array[Byte]): BaseFreqSketchImpl = {
    FreqSketch(bytes, StringType)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Computes the approximate "heaviest" or "most frequently occurring"
      items from an input sketch state.
  """,
  // group = "math_funcs",
  since = "3.1.2")
case class FreqItemFromSketchState(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Logging {

  override def prettyName: String = "approx_freqitems_estimate"

  override lazy val dataType: DataType = {
    ArrayType(StructType(Seq(StructField("item", StringType), StructField("estimated", LongType))))
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  private def getFreqItems(buffer: BaseFreqSketchImpl): Any = {
    if (!buffer.isEmpty) {
      val freqItems = buffer.getFrequentItems().map { case (item, estimate) =>
        InternalRow(UTF8String.fromString(item.asInstanceOf[String]), estimate)
      }
      new GenericArrayData(freqItems)
    } else {
      null
    }
  }

  @transient private[this] lazy val getOutputFreqItems = {
    (ar: Any) => try {
      getFreqItems(FreqSketch(ar.asInstanceOf[Array[Byte]], StringType))
    } catch {
      case NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${FreqSketch(child.dataType).getClass.getSimpleName} sketch data.")
        null
    }
  }

  override def nullSafeEval(ar: Any): Any = getOutputFreqItems(ar)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val pf = ctx.addReferenceObj("getFreqItems", getOutputFreqItems,
      classOf[Any => Any].getCanonicalName)
    val freqItems = ctx.freshName("freqItems")
    nullSafeCodeGen(ctx, ev, ar => {
      s"""
         |Object $freqItems = $pf.apply($ar);
         |if ($freqItems != null) {
         |  ${ev.value} = (${javaType(dataType)}) $freqItems;
         |} else {
         |  ${ev.isNull} = true;
         |}
       """.stripMargin
    })
  }
}
