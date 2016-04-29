/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 *
 * BSD 3-Clause License
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.alpine.plugin.samples.ver1_0

import com.alpine.model.RegressionRowModel
import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.model.RegressionModelWrapper
import com.alpine.transformer.RegressionTransformer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, sql}

import scala.collection.mutable

/**
  * This is the design-time code for the Regression evaluator operator.
  * It takes an input dataset and a Regression model to create a set of metrics
  * evaluating the quality of the model on the dataset.
  *
  * The result is a dataset containing one row, where each statistic is in a separate column.
  *
  * N.B. The "explainedVariance" statistic may be inconsistent with the usual definition.
  * See https://issues.apache.org/jira/browse/SPARK-9005
  * (fixed in Spark 1.5.0, but we are currently using Spark 1.3.1).
  */
class RegressionEvaluatorSignature extends OperatorSignature[
  RegressionEvaluatorGUINode,
  RegressionEvaluatorRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - Regression Evaluator",
      category = "Plugin Sample - Spark",
      author = "Jenny Thompson",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class RegressionEvaluatorGUINode extends OperatorGUINode[
  Tuple2[HdfsTabularDataset, RegressionModelWrapper],
  HdfsTabularDataset
  ] {

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

    val columnDefs = Array(
      ColumnDef("explainedVariance", ColumnType.Double),
      ColumnDef("meanAbsoluteError", ColumnType.Double),
      ColumnDef("meanSquaredError", ColumnType.Double),
      ColumnDef("rootMeanSquaredError", ColumnType.Double),
      ColumnDef("r2", ColumnType.Double)
    )

    val expectedOutputFormat = TabularFormatAttributes.createTSVFormat()

    val outputSchema = TabularSchema(columnDefs, expectedOutputFormat)
    operatorSchemaManager.setOutputSchema(outputSchema)
  }

}


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
    val sparkUtils = new SparkRuntimeUtils(
      sparkContext
    )
    val inputDataSet = input._1
    val schemaFixedColumns = inputDataSet.tabularSchema.getDefinedColumns

    val dataFrame: DataFrame = sparkUtils.getDataFrame(inputDataSet)

    val model: RegressionRowModel = input._2.model

    val resultDataFrame: DataFrame = RegressionEvaluatorUtil.calculateResultDataFrame(
      sparkContext,
      schemaFixedColumns,
      dataFrame,
      model,
      listener
    )

    RegressionEvaluatorUtil.saveOutput(
      sparkContext,
      operatorParameters,
      listener,
      sparkUtils,
      resultDataFrame
    )
  }

}

object RegressionEvaluatorUtil {

  /**
    * Given the transformer from the model, and the independent and dependent columns
    * from the input data. Returns a tuple with the predicted value (generated from the  transformer)
    * and the actual value. (The dependent column).
    */
  def calculatePredictionTuple(independentColumnIndices: Array[Int],
                               dependentColumnIndex: Int,
                               transformer: RegressionTransformer)(row: sql.Row): (Double, Double) = {
    val inputRow: Array[Any] = getInputRowForModel(independentColumnIndices, row)
    val observedValue = row(dependentColumnIndex).asInstanceOf[Number].doubleValue()
    val predictedValue = transformer.predict(inputRow)
    (predictedValue, observedValue)
  }

  /**
    * Converts from the Spark SQL row in the input data frame to an array of the input features.
    */
  def getInputRowForModel(independentColumnIndices: Array[Int], row: sql.Row): Array[Any] = {
    val inputRow = Array.ofDim[Any](independentColumnIndices.length)
    var i = 0
    while (i < inputRow.length) {
      // TODO: (Warning).
      // This code will work for simple types, but not Sparse or non ISO Datetime.
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

  /**
    * Use the model to predict on the data, then compute the regression evaluation
    * metrics against the known value of the dependent column in the input data.
    * Return a DataFrame with the results of each metric.
    */
  def calculateResultDataFrame(sparkContext: SparkContext,
                               schemaFixedColumns: Seq[ColumnDef],
                               dataFrame: DataFrame,
                               model: RegressionRowModel,
                               listener: OperatorListener): DataFrame = {
    val inputFeatures = model.inputFeatures

    /**
      * Build a map from the column names in the input dataset to their corresponding indices in that dataset.
      */
    val nameToIndexMap: Map[String, Int] = schemaFixedColumns.zipWithIndex.map(t => (t._1.columnName, t._2)).toMap
    /**
      * We match only on the name and not the whole ColumnDef to allow leniency in the types.
      * e.g. A Linear Regression takes Double input columns, but the data-frame has Long types.
      */
    val independentColumnIndices = inputFeatures.map(feature => {
      val index: Option[Int] = nameToIndexMap.get(feature.columnName)
      if (index.isEmpty) {
        val errorMessage =
          s"""Cannot find the column with name
              |${feature.columnName}
              | needed for prediction in the input dataset.""".stripMargin
        listener.notifyError(errorMessage)
        throw new IllegalArgumentException(errorMessage)
      }
      index.get
    }).toArray

    val dependentColumnIndex = schemaFixedColumns
      .indexWhere(columnDef => columnDef.columnName == model.dependentFeature.columnName)
    if (dependentColumnIndex == -1) {
      val errorMessage =
        s"""Cannot find the column with name
            |${model.dependentFeature.columnName}
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

  /**
    * Uses the spark utils class to save the result dataFrame to HDFS.
    */
  def saveOutput(sparkContext: SparkContext,
                 operatorParameters: OperatorParameters,
                 listener: OperatorListener,
                 sparkUtils: SparkRuntimeUtils,
                 resultDataFrame: DataFrame): HdfsDelimitedTabularDataset = {
    val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)
    listener.notifyMessage("Output path is : " + outputPathStr)
    val driverHdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    val outputPath = new Path(outputPathStr)
    if (driverHdfs.exists(outputPath)) {
      listener.notifyMessage("Deleting previous output.")
      driverHdfs.delete(outputPath, true)
    }
    sparkUtils.saveAsTSV(outputPathStr, resultDataFrame, Some(operatorParameters.operatorInfo))
  }

}

