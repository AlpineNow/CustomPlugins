/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.samples.ver1_0

import scala.collection.mutable

import com.alpine.model.RegressionRowModel
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils.SparkUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.model.RegressionModelWrapper
import com.alpine.transformer.RegressionTransformer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, sql}

/**
 * This is the runtime code for the Regression evaluator operator.
 * It takes an input dataset and a Regression model to create a RDD[(Double, Double)],
 * of predicted and observed values.
 * It then calls RegressionMetrics from MLLib to create a set of metrics
 * evaluating the quality of the model on the dataset.
 *
 * It returns an RDD containing one row, where each statistic is in a separate column.
 */
class RegressionEvaluatorRuntime extends SparkRuntimeWithIOTypedJob[
  RegressionEvaluatorJob,
  Tuple2[HdfsTabularDataset, RegressionModelWrapper],
  HdfsTabularDataset
  ]

class RegressionEvaluatorJob extends
SparkIOTypedPluginJob[
  Tuple2[HdfsTabularDataset, RegressionModelWrapper],
  HdfsTabularDataset
  ] {
  override def onExecution(sparkContext: SparkContext,
                           appConf: mutable.Map[String, String],
                           input: Tuple2[HdfsTabularDataset, RegressionModelWrapper],
                           operatorParameters: OperatorParameters,
                           listener: OperatorListener): HdfsTabularDataset = {
    val sparkUtils = new SparkUtils(
      sparkContext
    )
    val inputDataSet = input.getT1()
    val schemaFixedColumns = inputDataSet.getTabularSchema().getDefinedColumns()

    val dataFrame: DataFrame = sparkUtils.getDataFrame(inputDataSet)

    val model: RegressionRowModel = input.getT2().model

    val resultDataFrame: DataFrame = RegressionEvaluatorUtil.calculateResultDataFrame(
      sparkContext,
      schemaFixedColumns,
      dataFrame,
      model,
      listener
    )

    RegressionEvaluatorUtil.saveOutput(
      sparkContext,
      HdfsParameterUtils.getOutputPath(operatorParameters),
      listener,
      sparkUtils,
      resultDataFrame
    )
  }

}

object RegressionEvaluatorUtil {

  def calculatePredictionTuple(independentColumnIndices: Array[Int],
                               dependentColumnIndex: Int,
                               transformer: RegressionTransformer)(row: sql.Row): (Double, Double) = {
    val inputRow: Array[Any] = getInputRowForModel(independentColumnIndices, row)
    val observedValue = row(dependentColumnIndex).asInstanceOf[Number].doubleValue()
    val predictedValue = transformer.predict(inputRow)
    (predictedValue, observedValue)
  }

  def getInputRowForModel(independentColumnIndices: Array[Int], row: sql.Row): Array[Any] = {
    val inputRow = Array.ofDim[Any](independentColumnIndices.length)
    var i = 0
    while (i < inputRow.length) {
      // TODO: Handle type conversion between SparkSQL types and Model types.
      // This code will work if the types happen to match (e.g. are numeric or String),
      // but not in other cases (e.g. Boolean, Sparse etc).
      inputRow(i) = row(independentColumnIndices(i))
      i += 1
    }
    inputRow
  }

  def getOutputRow(metrics: RegressionMetrics): sql.Row = {
    val values = (metrics.explainedVariance,
      metrics.meanAbsoluteError,
      metrics.meanSquaredError,
      metrics.rootMeanSquaredError,
      metrics.r2)
    sql.Row.fromTuple(values)
  }

  def calculateResultDataFrame(sparkContext: SparkContext,
                               schemaFixedColumns: Seq[ColumnDef],
                               dataFrame: DataFrame,
                               model: RegressionRowModel,
                               listener: OperatorListener): DataFrame = {
    val inputFeatures = model.inputFeatures
    val independentColumnIndices = inputFeatures.map(feature => {
      val index = schemaFixedColumns.indexWhere(columnDef => columnDef.columnName == feature.name)
      if (index == -1) {
        val errorMessage =
          s"""Cannot find the column with name
             |${feature.name}
             | needed for prediction in the input dataset.""".stripMargin
        listener.notifyError(errorMessage)
        throw new IllegalArgumentException(errorMessage)
      }
      index
    }).toArray

    val dependentColumnIndex = schemaFixedColumns
      .indexWhere(columnDef => columnDef.columnName == model.dependentFeature.name)
    if (dependentColumnIndex == -1) {
      val errorMessage =
        s"""Cannot find the column with name
           |${model.dependentFeature.name}
           | (the dependent column of the model) in the input dataset.""".stripMargin
      listener.notifyError(errorMessage)
      throw new IllegalArgumentException(errorMessage)
    }

    val transformer = model.transformer

    val predictionTuples: RDD[(Double, Double)] = dataFrame.map((row: sql.Row) => {
      RegressionEvaluatorUtil
        .calculatePredictionTuple(independentColumnIndices, dependentColumnIndex, transformer)(row)
    })

    val metrics: RegressionMetrics = new RegressionMetrics(predictionTuples)

    val outputRow = RegressionEvaluatorUtil.getOutputRow(metrics)
    val outputRDD = sparkContext.makeRDD(Seq(outputRow))
    val sqlContext = new SQLContext(sparkContext)

    val schema = StructType(
      Seq(
        StructField("explainedVariance", DoubleType, nullable = false),
        StructField("meanAbsoluteError", DoubleType, nullable = false),
        StructField("meanSquaredError", DoubleType, nullable = false),
        StructField("rootMeanSquaredError", DoubleType, nullable = false),
        StructField("r2", DoubleType, nullable = false)
      )
    )
    val resultDataFrame = sqlContext.createDataFrame(outputRDD, schema)
    resultDataFrame
  }

  // TODO: Move to a plugin-spark for use by all operators.
  def saveOutput(sparkContext: SparkContext,
                 outputPathStr: String,
                 listener: OperatorListener,
                 sparkUtils: SparkUtils,
                 resultDataFrame: DataFrame): HdfsDelimitedTabularDataset = {
    listener.notifyMessage("Output path is : " + outputPathStr)
    val driverHdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    val outputPath = new Path(outputPathStr)
    if (driverHdfs.exists(outputPath)) {
      listener.notifyMessage("Deleting previous output.")
      driverHdfs.delete(outputPath, true)
    }
    sparkUtils.saveAsTSV(outputPathStr, resultDataFrame)
  }

}