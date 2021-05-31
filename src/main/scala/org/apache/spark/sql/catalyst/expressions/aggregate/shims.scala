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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

object DataSketches {
  import FunctionRegistoryShim._

  private var _installed = false

  val sketchExprs = Seq(
    expression[QuantileSketch]("approx_percentile_ex"),
    expression[KllFloatsSketch]("approx_percentile_kll"),
    expression[ReqSketch]("approx_percentile_req"),
    expression[SketchQuantile]("approx_percentile_accumulate"),
    expression[CombineQuantileSketches]("approx_percentile_combine"),
    expression[FromQuantileSketch]("approx_percentile_estimate")
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

  private val FUNC_ALIAS = TreeNodeTag[String]("functionAliasName")

  /** See usage above. */
  private[aggregate] def expression[T <: Expression](name: String, setAlias: Boolean = false)
      (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      tag.runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        try {
          val exp = varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
          if (setAlias) exp.setTagValue(FUNC_ALIAS, name)
          exp
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount).distinct.sorted
          val invalidArgumentsMsg = if (validParametersCount.length == 0) {
            s"Invalid arguments for function $name"
          } else {
            val expectedNumberOfParameters = if (validParametersCount.length == 1) {
              validParametersCount.head.toString
            } else {
              validParametersCount.init.mkString("one of ", ", ", " and ") +
                validParametersCount.last
            }
            s"Invalid number of arguments for function $name. " +
              s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
          }
          throw new AnalysisException(invalidArgumentsMsg)
        }
        try {
          val exp = f.newInstance(expressions : _*).asInstanceOf[Expression]
          if (setAlias) exp.setTagValue(FUNC_ALIAS, name)
          exp
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }
    (name, (expressionInfo[T](name), builder))
  }

  /**
   * Creates an [[ExpressionInfo]] for the function as defined by expression T using the given name.
   */
  private def expressionInfo[T <: Expression : ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.group(),
          df.since(),
          df.deprecated())
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }
}
