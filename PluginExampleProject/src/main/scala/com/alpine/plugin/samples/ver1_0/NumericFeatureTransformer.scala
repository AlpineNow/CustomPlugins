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

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.SparkJobConfiguration
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.SparkParameterUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import com.alpine.plugin.core.{OperatorMetadata, _}
import org.apache.spark.sql.DataFrame

class NumericFeatureTransformerSignature extends OperatorSignature[
  NumericFeatureTransformerGUINode,
  NumericFeatureTransformerRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - Numeric Feature Transformer",
      category = "Plugin Sample - Spark",
      author = "Sung Chung",
      version = 1,
      helpURL = "http://alpinenow.com",
      iconNamePrefix = "test"
    )
  }
}

class NumericFeatureTransformerGUINode
  extends SparkDataFrameGUINode[NumericFeatureTransformerJob] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    import NumericFeatureTransformerUtil._

    //add a checkbox so the user can select the columns to transform
    operatorDialog.addTabularDatasetColumnCheckboxes(
      columnsToTransformKey,
      "Columns to transform",
      ColumnFilter.NumericOnly,
      "main"
    )

    //add a drop down box so that the user can select whether to do a square (pow2) or cubic (pow3)
    //transformation
    operatorDialog.addDropdownBox(
      transformationTypeKey,
      "Transformation type",
      Array(pow2, pow3),
      pow2
    )

    //call the super method which adds the standard Hadoop storage output parameters.
    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)

    //add the standard spark configuration parameters
    SparkParameterUtils.addStandardSparkOptions(
      operatorDialog,
      defaultNumExecutors = 2,
      defaultExecutorMemoryMB = 1024,
      defaultDriverMemoryMB = 1024,
      defaultNumExecutorCores = 1
    )
  }

  /**
    * Define the columns in the output schema.
    * The resulting dataset will have all the same columns as the input dataset, but with the
    * addition of each column to selected to transform named with the original column name,
    * underscore, the transformation type.
    * I.e. If the user selected columns 'ColB' and 'ColC' from a dataset with columns
    * 'ColA', 'ColB', 'ColC' and the 'pow2' transformation type, the result dataset would have
    * columns: 'ColA', 'ColB', 'ColC', 'ColB_pow2', 'ColC_pow2'
    */
  override def defineOutputSchemaColumns(inputSchema: TabularSchema,
                                         parameters: OperatorParameters): Seq[ColumnDef] = {

    val columnsToTransform = NumericFeatureTransformerUtil.getColumnsToTransform(parameters)
    val transformationType = NumericFeatureTransformerUtil.getTransformationType(parameters)

    inputSchema.getDefinedColumns ++
      columnsToTransform.map(column => {
        ColumnDef(
          column + "_" + transformationType,
          ColumnType.Double
        )
      })
  }

  override def onOutputVisualization(params: OperatorParameters,
                                     output: HdfsTabularDataset,
                                     visualFactory: VisualModelFactory): VisualModel = {
    //create the standard visualization of the output data
    val datasetVisualModel = visualFactory.createTabularDatasetVisualization(output)
    val addendum: Map[String, AnyRef] = output.addendum
    val addendumVisualModel =
      visualFactory.createTextVisualization(
        //the output contains the map of the visual information returned by the dataFrameJob
        "Data transformation type completed:  " + addendum(
          NumericFeatureTransformerUtil.transformationTypeVisualKey).toString + " .\n" +
          "The resulting data has " + addendum(
          NumericFeatureTransformerUtil.dataFrameLengthVisualKey).toString + " rows."
      )
    val compositeVisualModel = visualFactory.createCompositeVisualModel()
    compositeVisualModel.addVisualModel("Dataset", datasetVisualModel)
    compositeVisualModel.addVisualModel("Addendum", addendumVisualModel)
    compositeVisualModel
  }
}

class NumericFeatureTransformerRuntime
  extends SparkDataFrameRuntime[NumericFeatureTransformerJob] {

  override def getSparkJobConfiguration(parameters: OperatorParameters, input: HdfsTabularDataset): SparkJobConfiguration = {
    val config = super.getSparkJobConfiguration(parameters, input)
    config.additionalParameters += ("spark.shuffle.memoryFraction" -> "0.1")
    config
  }

}

class NumericFeatureTransformerJob extends SparkDataFrameJob {
  /**
    * Returns a tuple with the transformed dataFrame and a map containing two
    * additional pieces of information: the type of data transformation completed,
    * and the number of rows in the resulting DataFrame. That extra information is used to create a
    * second visualization for the plugin (in addition to a preview of the result dataset)
    * in the GUI node with the addendum information.
    */
  override def transformWithAddendum(operatorParameters: OperatorParameters,
                                     dataFrame: DataFrame,
                                     sparkUtils: SparkRuntimeUtils,
                                     listener: OperatorListener): (DataFrame, Map[String, AnyRef]) = {

    val columnsToTransform = NumericFeatureTransformerUtil.getColumnsToTransform(operatorParameters)
    val transformationType = NumericFeatureTransformerUtil.getTransformationType(operatorParameters)

    listener.notifyMessage("Columns to transform are : " + columnsToTransform.mkString(","))
    val transformedDataFrame =
      if (NumericFeatureTransformerUtil.pow2.equals(transformationType)) {
        dataFrame.selectExpr(
          dataFrame.columns.map("`" + _ + "`") ++
            columnsToTransform.map(colName =>
              "(`" + colName + "` * `" + colName + "`)" + " as `" + colName + "_" +
                transformationType + "`"): _*
        )
      } else {
        dataFrame.selectExpr(
          dataFrame.columns.map("`" + _ + "`") ++
            columnsToTransform.map(colName =>
              "(`" + colName + "` * `" + colName + "` * `" + colName + "`)" + " as `" + colName +
                "_" + transformationType + "`"
            ): _*
        )
      }
    //In order to create a visualization with the number of rows in the result and the type of
    //transformation that we used, so we add that information to the addendum.
    val dataFrameSize = transformedDataFrame.count.toString
    val addendum = Map(
      NumericFeatureTransformerUtil.transformationTypeVisualKey -> transformationType,
      NumericFeatureTransformerUtil.dataFrameLengthVisualKey -> new Integer(dataFrameSize))
    (transformedDataFrame, addendum)
  }

}

/**
  * This is a utility object to keep track of things that need to be used across the plugin classes.
  * e.g. The String keys for parameters.
  */
object NumericFeatureTransformerUtil {
  val columnsToTransformKey = "columnsToTransform"
  val transformationTypeKey = "transformationType"
  val pow2 = "Pow2"
  val pow3 = "Pow3"

  val dataFrameLengthVisualKey = "dataFrameLength"
  val transformationTypeVisualKey = "transformationType"

  def getTransformationType(parameters: OperatorParameters): String = {
    parameters.getStringValue(transformationTypeKey)
  }

  def getColumnsToTransform(parameters: OperatorParameters): Seq[String] = {
    parameters.getTabularDatasetSelectedColumns(columnsToTransformKey)._2
  }
}
