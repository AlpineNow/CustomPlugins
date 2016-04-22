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

import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, HdfsTabularDataset, OperatorSchemaManager}
import com.alpine.plugin.core.spark.utils.{MLlibUtils, SparkRuntimeUtils}
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.SparkParameterUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import com.alpine.plugin.model.RegressionModelWrapper
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LassoWithSGD

import scala.collection.mutable

/**
  * This is an example plugin showing how to use MLlib to train a basic Linear Regression Model.
  * The output of this operator can be connected to the "Regression Evaluator" plugin,
  * or Alpine's "Predictor" operator.
  *
  * In this example we show how to use one of Alpine's predefined Model types,
  * the LinearRegressionModel. See the ExampleClassificationModel for an example of creating
  * a new model type.
  *
  * N.B. The algorithm used works best on normalized input data.
  */
class LinearRegressionSignature extends OperatorSignature[
  LinearRegressionPluginGUINode,
  LinearRegressionPluginRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - Linear Regression",
      category = "Plugin Sample - Spark",
      author = "Jenny Thompson",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class LinearRegressionPluginGUINode extends OperatorGUINode[
  HdfsTabularDataset,
  RegressionModelWrapper] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnDropdownBox(
      "dependentColumn",
      "Dependent Column",
      ColumnFilter.NumericOnly,
      "main"
    )

    operatorDialog.addTabularDatasetColumnCheckboxes(
      "independentColumns",
      "Independent Columns",
      ColumnFilter.NumericOnly,
      "main"
    )

    SparkParameterUtils.addStandardSparkOptions(
      operatorDialog,
      defaultNumExecutors = 2,
      defaultExecutorMemoryMB = 1024,
      defaultDriverMemoryMB = 1024,
      defaultNumExecutorCores = 1
    )
  }


  override def onOutputVisualization(params: OperatorParameters,
                                     output: RegressionModelWrapper,
                                     visualFactory: VisualModelFactory): VisualModel = {
    val model = output.model.asInstanceOf[LinearRegressionModel]
    val eqn = model.dependentFeatureName + " = " + model.intercept + " + " +
      model.coefficients.zip(model.inputFeatures).map(t => s"${t._1} * ${t._2.columnName}").mkString(" + ")
    val text: String = s"Model is \n $eqn"
    visualFactory.createTextVisualization(text)
  }
}

class LinearRegressionPluginRuntime extends SparkRuntimeWithIOTypedJob[
  LinearRegressionTrainingJob,
  HdfsTabularDataset,
  RegressionModelWrapper]

class LinearRegressionTrainingJob extends SparkIOTypedPluginJob[
  HdfsTabularDataset,
  RegressionModelWrapper] {
  override def onExecution(sparkContext: SparkContext,
                           appConf: mutable.Map[String, String],
                           input: HdfsTabularDataset,
                           operatorParameters: OperatorParameters,
                           listener: OperatorListener): RegressionModelWrapper = {
    val sparkUtils = new SparkRuntimeUtils(sparkContext)

    val dependentColumn = operatorParameters.getTabularDatasetSelectedColumn("dependentColumn")._2
    val independentColumnNames = operatorParameters.getTabularDatasetSelectedColumns("independentColumns")._2

    val schemaFixedColumns = input.tabularSchema.getDefinedColumns

    // If there are a large number of columns, it would be a good idea to create a map from the column name to the
    // index of that column before looking up the index
    // (it would bring the complexity from n^2 to n, where n is the number of columns).
    val independentColumnIndices = independentColumnNames.map(name => {
      schemaFixedColumns.indexWhere(columnDef => columnDef.columnName == name)
    })

    //find the index fo the dependent column
    val dependentColumnIndex = schemaFixedColumns
      .indexWhere(columnDef => columnDef.columnName == dependentColumn)

    val inputDataFrame = sparkUtils.getDataFrame(input)
    val labeledPoints = inputDataFrame.map(MLlibUtils.toLabeledPoint(dependentColumnIndex, independentColumnIndices))
    //.persist(StorageLevel.MEMORY_AND_DISK) // Could perform caching here.

    // This is different to the internal Alpine implementation of Linear Regression, which uses OWLQN,
    // but it is also simpler to read.
    // Various other optimizations can be used here.
    val mlLibModel = new LassoWithSGD().run(labeledPoints)
    // Use "train" to train with parameters.
    // e.g.
    // val mlLibModel = LassoWithSGD.train(labeledPoints, 10, 1.0, 0.0, 1.0)


    val mLlibCoefficients: Array[Double] = mlLibModel.weights.toArray
    val independentColumnDefs: Array[ColumnDef] = independentColumnNames.map(f => ColumnDef(f, ColumnType.Double))
    val alpineLinearRegressionModel: LinearRegressionModel =
      LinearRegressionModel.make(
        mLlibCoefficients, independentColumnDefs,
        mlLibModel.intercept, dependentColumn
      )

    //return a RegressionModelWrapper containing the alpineLinearRegressionModel
    new RegressionModelWrapper(
      "Simple Linear Regression Model",
      alpineLinearRegressionModel,
      Some(operatorParameters.operatorInfo)
    )
  }

}
