/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import scala.collection.mutable

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils.SparkUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkJobConfiguration, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HDFSParameterUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}
import com.alpine.plugin.core.{OperatorMetadata, _}
import org.apache.spark.SparkContext

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

class NumericFeatureTransformerGUINode extends OperatorGUINode[
  HdfsTabularDataset,
  HdfsTabularDataset] {
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnCheckboxes(
      "columnsToTransform",
      "Columns to transform",
      ColumnFilter.NumericOnly,
      "main"
    )

    operatorDialog.addDropdownBox(
      "transformationType",
      "Transformation type",
      Array("Pow2", "Pow3").toSeq,
      "Pow2"
    )

    operatorDialog.addDropdownBox(
      "storageFormat",
      "Storage format",
      Array("Parquet", "Avro", "TSV").toSeq,
      "Parquet"
    )

    HDFSParameterUtils
          .addStandardHDFSOutputParameters(operatorDialog, operatorDataSourceManager)
  }

  private def updateOutputSchema(
    inputSchemas: mutable.Map[String, TabularSchema],
    params: OperatorParameters,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    // There can only be one input schema.
    if (inputSchemas.size > 0) {
      val inputSchema = inputSchemas.values.iterator.next()
      if (inputSchema.getDefinedColumns().length > 0) {
        val numInputColumns = inputSchema.getNumDefinedColumns()
        val (_, columnsToTransform) =
          params.getTabularDatasetSelectedColumns("columnsToTransform")
        val transformationType = params.getStringValue("transformationType")
        val numTransformedColumns = columnsToTransform.length

        val columnDefs = new mutable.ArrayBuffer[ColumnDef]()
        columnDefs ++= inputSchema.getDefinedColumns()
        var i = 0
        while (i < columnsToTransform.length) {
          columnDefs +=
            ColumnDef(
              columnsToTransform(i) + "_" + transformationType,
              ColumnType.Double
            )
          i += 1
        }

        val outputSchema = TabularSchema(columnDefs)
        operatorSchemaManager.setOutputSchema(outputSchema)

        val storageFormat = params.getStringValue("storageFormat")
        if (storageFormat.equals("Parquet")) {
          outputSchema.setExpectedOutputFormat(
            TabularFormatAttributes.createParquetFormat()
          )
        } else if (storageFormat.equals("Avro")) {
          outputSchema.setExpectedOutputFormat(
            TabularFormatAttributes.createAvroFormat()
          )
        } else { // Storage format is TSV.
          outputSchema.setExpectedOutputFormat(
            TabularFormatAttributes.createDelimitedFormat(
              "\t",
              "\\",
              "\""
            )
          )
        }
      }
    }
  }

  override def onInputOrParameterChange(
    inputSchemas: mutable.Map[String, TabularSchema],
    params: OperatorParameters,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    this.updateOutputSchema(
      inputSchemas,
      params,
      operatorSchemaManager
    )
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

class NumericFeatureTransformerRuntime extends SparkRuntimeWithIOTypedJob[
  NumericFeatureTransformerJob,
  HdfsTabularDataset,
  HdfsTabularDataset] {

  override def getSparkJobConfiguration(parameters: OperatorParameters, input: HdfsTabularDataset): SparkJobConfiguration = {
    val config = super.getSparkJobConfiguration(parameters, input)
    config.additionalParameters += ("spark.shuffle.memoryFraction" -> "0.1")
    config
  }

}

class NumericFeatureTransformerJob extends
  SparkIOTypedPluginJob[HdfsTabularDataset, HdfsTabularDataset] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: HdfsTabularDataset,
    operatorParameters: OperatorParameters,
    listener: OperatorListener): HdfsTabularDataset = {
    val sparkUtils = new SparkUtils(
      sparkContext
    )
    val inputSchema = input.getTabularSchema()
    println("Input schema : ")
    inputSchema.getDefinedColumns().map(
      colDef =>
        println(colDef.columnName + " : " + colDef.columnType.name)
    )
    val dataFrame = sparkUtils.getDataFrame(
      input
    )

    listener.notifyMessage("Starting the feature transformer.")

    val (_, columnsToTransform) =
      operatorParameters.getTabularDatasetSelectedColumns("columnsToTransform")

    listener.notifyMessage("Features to transform are : " + columnsToTransform.mkString(","))

    val outputPathStr = HDFSParameterUtils.getOutputPath(operatorParameters)

    listener.notifyMessage("Output path is : " + outputPathStr)

    val storageFormat = operatorParameters.getStringValue("storageFormat")
    val transformationType = operatorParameters.getStringValue("transformationType")
    val transformedDataFrame =
      if (transformationType.equals("Pow2")) {
        dataFrame.selectExpr(
          dataFrame.columns.toSeq ++
            columnsToTransform.map(colName => "(" + colName + " * " + colName + ")" + " as " + colName + "_Pow2").toSeq : _*
        )
      } else {
        dataFrame.selectExpr(
          dataFrame.columns.toSeq ++
            columnsToTransform.map(colName => "(" + colName + " * " + colName + " * " + colName + ")" + " as " + colName + "_Pow3").toSeq : _*
        )
      }

    if (HDFSParameterUtils.getOverwriteParameterValue(operatorParameters)) {
      sparkUtils.deleteFilePathIfExists(outputPathStr)
    }

    val output: HdfsTabularDataset =
      if (storageFormat.equals("Parquet")) {
        sparkUtils.saveAsParquet(
          outputPathStr,
          transformedDataFrame
        )
      } else if (storageFormat.equals("Avro")) {
        sparkUtils.saveAsAvro(
          outputPathStr,
          transformedDataFrame
        )
      } else { // Storage format is TSV.
        sparkUtils.saveAsTSV(
          outputPathStr,
          transformedDataFrame
        )
      }

    output.setDictValue("TestValue1", new Integer(1))
    output.setDictValue("TestValue2", new Integer(2))

    output
  }
}
