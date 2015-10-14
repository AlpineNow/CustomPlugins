/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame, Column}
import org.apache.spark.sql.ExperimentalSqlTools // This is slightly evil
import org.apache.spark.sql.catalyst.expressions._

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.templates.{InferredSparkDataFrameJob,
  InferredSparkDataFrameGUINode, SparkDataFrameRuntime}

class InternalNormalizationSignature extends OperatorSignature[
  InternalNormalizationGUINode,
  InternalNormalizationRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "InternalNormalization",
      category = "Transformation",
      author = "Holden Karau",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class InternalNormalizationJob extends InferredSparkDataFrameJob {
  override def transform(operatorParameters: OperatorParameters, input: DataFrame,
    listener: OperatorListener):
      DataFrame = {
    val (_, columnsToTransform) =
      operatorParameters.getTabularDatasetSelectedColumns("columnsToTransform")
    val columns = columnsToTransform.toArray
    import InternalNormalizationTransformations._
    val transformation =
      InternalNormalizationTransformations.withName(operatorParameters.getStringValue("transformation"))
    transformation match {
      case DivideByAverage => divideByAverage(columns, input)
//      case Proportion => proportion(columns, input)
      case Range => range(columns, input)
      case Z => zScore(columns, input)
      case _ => throw new Exception("Un-implemented transformation")
    }
  }

  def range(columns: Array[String], input: DataFrame) = {
    val allColumns = input.columns
    val columnMap = columns.zipWithIndex.toMap
    // Compute the average, counts, and var of the columns we care about
    val minMaxExpressions = columns.flatMap{name =>
      val column = new Column(name)
      val expr = ExperimentalSqlTools.getExpr(column)
      List(new Column(Min(expr)),
        new Column(Max(expr))
      )
    }
    val minMaxs = input.agg(minMaxExpressions.head,
      minMaxExpressions.tail:_*).head()
    // Normalize the results
    val expressions = allColumns.map{name =>
      val column = new Column(name)
      // Keep original if not selected for transformation.
      columnMap.get(name) match {
        case Some(index) => {
          val expr = ExperimentalSqlTools.getExpr(column)
          // (e-min(e))/(max(e)-min(e))
          new Column(Divide(Subtract(expr, Literal.apply(minMaxs.get(2*index))),
            Subtract(Literal.apply(minMaxs.get(2*index+1)),
              Literal.apply(minMaxs.get(2*index)))
          )).as(name)
        }
        case None => column
      }
    }
    input.agg(expressions.head, expressions.tail:_*)
  }

  def zScore(columns: Array[String], input: DataFrame) = {
    val allColumns = input.columns
    val columnMap = columns.zipWithIndex.toMap
    // Compute the average, counts, and var of the columns we care about
    val avgVarExpressions = columns.flatMap{name =>
      val column = new Column(name)
      val expr = ExperimentalSqlTools.getExpr(column)

      //fixme: doesn't compile with Spark 1.5.x, has to comment this out and replace var with avg for now,
      //fixme: just to get it compile
    /*  List(new Column(Average(expr)),
        new Column(ExperimentalSqlTools.makeHiveUdaf("var", Seq(expr)))
      )*/

      List(new Column(Average(expr)),new Column(Average(expr)))
    }
    val avgVars = input.agg(avgVarExpressions.head,
      avgVarExpressions.tail:_*).head()
    // Normalize the results
    val expressions = allColumns.map{name =>
      val column = new Column(name)
      // Keep original if not selected for transformation.
      columnMap.get(name) match {
        case Some(index) => {
          val expr = ExperimentalSqlTools.getExpr(column)
          // (e-avg(e))/sqrt(var(e))
          new Column(Divide(Subtract(expr, Literal.apply(avgVars.get(2*index))),
            Sqrt(Literal.apply(avgVars.get(2*index+1))))).as(name)
        }
        case None => column
      }
    }
    input.agg(expressions.head, expressions.tail:_*)
  }

  def divideByAverage(columns: Array[String], input: DataFrame) = {
    val allColumns = input.columns
    val columnMap = columns.zipWithIndex.toMap
    // Compute the average of the columns we care about
    val averageExpressions = columns.map{name =>
      val column = new Column(name)
      new Column(Average(ExperimentalSqlTools.getExpr(column)))
    }
    val averages = input.agg(averageExpressions.head, averageExpressions.tail:_*).head()
    // Normalize the results
    val expressions = allColumns.map{name =>
      val column = new Column(name)
      // Keep original if not selected for transformation.
      columnMap.get(name) match {
        case Some(index) => {
          val expr = ExperimentalSqlTools.getExpr(column)
          // e/avg(e)
          new Column(Divide(expr, Literal.apply(averages.get(index)))).as(name)
        }
        case None => column
      }
    }
    input.agg(expressions.head, expressions.tail:_*)
  }
}

class InternalNormalizationRuntime extends SparkDataFrameRuntime[InternalNormalizationJob]

object InternalNormalizationTransformations extends Enumeration {
  type InternalNormalizationTransformations = Value
  val DivideByAverage, Proportion, Range, Z = Value
}

class InternalNormalizationGUINode extends InferredSparkDataFrameGUINode[InternalNormalizationJob] {
  override def onPlacement(operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    // Setup operator specific options
    operatorDialog.addTabularDatasetColumnCheckboxes(
      "columnsToTransform",
      "Columns to transform",
      ColumnFilter.NumericOnly,
      "main"
    )
    val transformations = InternalNormalizationTransformations.values.map(_.toString())
    operatorDialog.addDropdownBox(
      "method",
      "Method",
      transformations.toSeq,
      transformations.head)
    // Setup our standard DataFrame based options
    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

}
