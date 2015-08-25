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
      //the output contains the map of the visual information returned by the dataFrameJob
        "Data transformation type completed:  " + output.getDictValue(
          NumericFeatureTransformerUtil.transformationTypeVisualKey).toString + " .\n" +
        "The resulting data has " + output.getDictValue(
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
class NumericFeatureTransformerJob  extends SparkDataFrameJob {
  /**
   * Returns a tuple with the transformed dataFrame and a map containing two
   * additional pieces of information for visualization: the type of data
   * transformation completed, and the number of rows in the resulting DataFrame.
   */
  override def transformWithAddendum(operatorParameters: OperatorParameters,
                         dataFrame: DataFrame,
                         sparkUtils: SparkUtils,
                         listener: OperatorListener): (DataFrame , Map[String, AnyRef]) = {

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
    //we want to create a visualization with the number of rows in the result and the type of
    //transformation that we used, so we add that information to the outputVisualization map
    val dataFrameSize = dataFrame.count.toString
    val outputVisualization = Map(NumericFeatureTransformerUtil.transformationTypeVisualKey -> transformationType,
    NumericFeatureTransformerUtil.dataFrameLengthVisualKey -> new Integer(dataFrameSize))
    (dataFrame, outputVisualization)
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

  val dataFrameLengthVisualKey = "dataFrameLength"
  val transformationTypeVisualKey = "transformationType"
  def getTransformationType(parameters: OperatorParameters): String = {
    parameters.getStringValue(transformationTypeKey)
  }

  def getColumnsToTransform(parameters: OperatorParameters): Seq[String] = {
    parameters.getTabularDatasetSelectedColumns(columnsToTransformKey)._2
  }
}
