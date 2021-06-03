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
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataSketchConf._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait BaseFreqItemSketchAggregate extends TypedImperativeAggregate[ItemsSketch[String]] {

  override def createAggregationBuffer(): ItemsSketch[String] = {
    new ItemsSketch[String](SQLConf.get.frequentItemSketchMaxMapSize)
  }

  override def update(
      buffer: ItemsSketch[String],
      input: InternalRow): ItemsSketch[String] = {
    val value = children.head.eval(input)
    if (value != null) {
      buffer.update(value.asInstanceOf[UTF8String].toString)
    }
    buffer
  }

  override def merge(buffer: ItemsSketch[String], other: ItemsSketch[String])
      : ItemsSketch[String] = {
    buffer.merge(other)
    buffer
  }

  override def serialize(obj: ItemsSketch[String]): Array[Byte] = {
    obj.toByteArray(new ArrayOfStringsSerDe())
  }

  override def deserialize(bytes: Array[Byte]): ItemsSketch[String] = {
    ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe())
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
  since = "3.1.1")
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

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = {
    ArrayType(StructType(Seq(StructField("item", StringType), StructField("estimate", LongType))))
  }

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def eval(buffer: ItemsSketch[String]): Any = {
    val freqItems = buffer.getFrequentItems(ErrorType.NO_FALSE_POSITIVES).map { i =>
      InternalRow(UTF8String.fromString(i.getItem), i.getEstimate)
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
  since = "3.1.1")
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

  override def children: Seq[Expression] = child :: Nil

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = BinaryType

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def eval(buffer: ItemsSketch[String]): Any = {
    buffer.toByteArray(new ArrayOfStringsSerDe())
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Combines (i.e. merges) multiple input sketch states into a single output state.
  """,
  group = "agg_funcs",
  since = "3.1.1")
case class CombineFreqItemSketches(
    child: Expression,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[ItemsSketch[String]]
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

  override def createAggregationBuffer(): ItemsSketch[String] = {
    new ItemsSketch[String](SQLConf.get.frequentItemSketchMaxMapSize)
  }

  override def update(
      buffer: ItemsSketch[String],
      input: InternalRow): ItemsSketch[String] = {
    try {
      val bytes = child.eval(input).asInstanceOf[Array[Byte]]
      buffer.merge(ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe()))
    } catch {
      case e @ NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${classOf[ItemsSketch[_]].getSimpleName} sketch data.")
        throw e
    }
    buffer
  }

  override def merge(buffer: ItemsSketch[String], other: ItemsSketch[String])
      : ItemsSketch[String] = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: ItemsSketch[String]): Any = {
    buffer.toByteArray(new ArrayOfStringsSerDe())
  }

  override def serialize(obj: ItemsSketch[String]): Array[Byte] = {
    obj.toByteArray(new ArrayOfStringsSerDe())
  }

  override def deserialize(bytes: Array[Byte]): ItemsSketch[String] = {
    ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe())
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(col) - Computes the approximate "heaviest" or "most frequently occurring"
      items from an input sketch state.
  """,
  // group = "math_funcs",
  since = "3.1.1")
case class FreqItemFromSketchState(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Logging {

  override def prettyName: String = "approx_freqitems_estimate"

  override lazy val dataType: DataType = {
    ArrayType(StructType(Seq(StructField("item", StringType), StructField("estimate", LongType))))
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  private def getFreqItems(buffer: ItemsSketch[String]): Any = {
    if (!buffer.isEmpty) {
      val freqItems = buffer.getFrequentItems(ErrorType.NO_FALSE_POSITIVES).map { i =>
        InternalRow(UTF8String.fromString(i.getItem), i.getEstimate)
      }
      new GenericArrayData(freqItems)
    } else {
      null
    }
  }

  @transient private[this] lazy val getOutputFreqItems = {
    (ar: Any) => try {
      val bytes = ar.asInstanceOf[Array[Byte]]
      getFreqItems(ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe()))
    } catch {
      case NonFatal(_) =>
        logWarning("Illegal input bytes found, so cannot update " +
          s"an immediate ${classOf[ItemsSketch[_]].getSimpleName} sketch data.")
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
