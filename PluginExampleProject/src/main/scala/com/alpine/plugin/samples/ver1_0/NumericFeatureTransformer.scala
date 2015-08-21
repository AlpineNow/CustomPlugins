/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.SparkJobConfiguration
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import com.alpine.plugin.core.{OperatorMetadata, _}
import org.apache.spark.sql.DataFrame

class NumericFeatureTransformerSignature extends OperatorSignature[
  NumericFeatureTransformerGUINode,
  NumericFeatureTransformerRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "NumericFeatureTransformer",
      category = "Transformation",
      author = "Sung Chung",
      version = 1,
      helpURL = "http://www.nytimes.com",
      iconNamePrefix = "test"
    )
  }
}

class NumericFeatureTransformerGUINode
  extends SparkDataFrameGUINode[NumericFeatureTransformerJob] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {

    import NumericFeatureTransformerUtil._

    operatorDialog.addTabularDatasetColumnCheckboxes(
      columnsToTransformKey,
      "Columns to transform",
      ColumnFilter.NumericOnly,
      "main"
    )

    operatorDialog.addDropdownBox(
      transformationTypeKey,
      "Transformation type",
      Array(pow2, pow3),
      pow2
    )
    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

  override def defineOutputSchemaColumns(inputSchema: TabularSchema,
                                         parameters: OperatorParameters): Seq[ColumnDef] = {

    val columnsToTransform = NumericFeatureTransformerUtil.getColumnsToTransform(parameters)
    val transformationType = NumericFeatureTransformerUtil.getTransformationType(parameters)

    inputSchema.getDefinedColumns() ++
      columnsToTransform.map(column => {
        ColumnDef(
          column + "_" + transformationType,
          ColumnType.Double
        )
      })
  }

  override def onOutputVisualization(
    params: OperatorParameters,
    output: HdfsTabularDataset,
    visualFactory: VisualModelFactory): VisualModel = {
    val datasetVisualModel = visualFactory.createTabularDatasetVisualization(output)
    val addendumVisualModel =
      visualFactory.createTextVisualization(
        output.getDictValue("TestValue1").toString + " " +
        output.getDictValue("TestValue2").toString
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

class NumericFeatureTransformerJob  extends SparkDataFrameJob {

  override def transform(operatorParameters: OperatorParameters,
                         dataFrame: DataFrame,
                         sparkUtils: SparkUtils,
                         listener: OperatorListener): DataFrame = {

    val columnsToTransform = NumericFeatureTransformerUtil.getColumnsToTransform(operatorParameters)
    val transformationType = NumericFeatureTransformerUtil.getTransformationType(operatorParameters)

    listener.notifyMessage("Columns to transform are : " + columnsToTransform.mkString(","))
    if (NumericFeatureTransformerUtil.pow2.equals(transformationType)) {
      dataFrame.selectExpr(
        dataFrame.columns.toSeq ++
          columnsToTransform.map(colName =>
            "(" + colName + " * " + colName + ")" + " as " + colName + "_" + transformationType
          ): _*
      )
    } else {
      dataFrame.selectExpr(
        dataFrame.columns.toSeq ++
          columnsToTransform.map(colName =>
            "(" + colName + " * " + colName + " * " + colName + ")" + " as " + colName + "_" + transformationType
          ): _*
      )
    }
  }

  override def saveResults(transformedDataFrame: DataFrame,
                           sparkUtils: SparkUtils,
                           storageFormat: String,
                           outputPath: String,
                           overwrite: Boolean): HdfsTabularDataset = {
    val output = super.saveResults(transformedDataFrame, sparkUtils, storageFormat, outputPath, overwrite)
    /**
     * This is to show how information can be added to the output to be used for the visualization.
     * This information is retrieved in [[NumericFeatureTransformerGUINode.onOutputVisualization()]]
     */
    output.setDictValue("TestValue1", new Integer(1))
    output.setDictValue("TestValue2", new Integer(2))
    output
  }
}

/**
 * This is just a utility class to keep track of things that need to be used across the plugin classes.
 * e.g. The String keys for parameters.
 */
object NumericFeatureTransformerUtil {
  val columnsToTransformKey = "columnsToTransform"
  val transformationTypeKey = "transformationType"
  val pow2 = "Pow2"
  val pow3 = "Pow3"

  def getTransformationType(parameters: OperatorParameters): String = {
    parameters.getStringValue(transformationTypeKey)
  }

  def getColumnsToTransform(parameters: OperatorParameters): Seq[String] = {
    parameters.getTabularDatasetSelectedColumns(columnsToTransformKey)._2
  }
}
