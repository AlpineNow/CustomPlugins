package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils.SparkUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.mutable

class ColumnFilterPlugin {

}

class ColumnFilterSignature extends OperatorSignature[
  ColumnFilterGUINode,
  ColumnFilterRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Example ColumnFilter (Spark)",
      category = "Transformation",
      author = "Egor Pakhomov",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}


class ColumnFilterGUINode extends
OperatorGUINode[HdfsTabularDataset, HdfsTabularDataset] {
  override def onPlacement(
                            operatorDialog: OperatorDialog,
                            operatorDataSourceManager: OperatorDataSourceManager,
                            operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnCheckboxes(
      "columnsToKeep",
      "Columns to keep",
      ColumnFilter.All,
      "main"
    )

    operatorDialog.addDropdownBox(
      "storageFormat",
      "Storage format",
      Array("Parquet", "Avro", "Tsv").toSeq,
      "Parquet"
    )

    operatorDialog.addHdfsFileSelector(
      "outputPath",
      "Output Path",
      "/tmp",
      operatorDataSourceManager
    )

  }


  override def onInputOrParameterChange(inputSchemas: mutable.Map[String, TabularSchema],
                                         params: OperatorParameters,
                                         operatorSchemaManager: OperatorSchemaManager): Unit = {

    if (inputSchemas.nonEmpty) {
      val inputSchema = inputSchemas.values.iterator.next()

      if (inputSchema.getDefinedColumns().nonEmpty) {
        val (_, columnsToKeepArray) =
          params.getTabularDatasetSelectedColumns("columnsToKeep")
        val columnsToKeep = columnsToKeepArray.toSet
        val outputSchema =
          TabularSchema(
            inputSchema.getDefinedColumns().filter(colDef => columnsToKeep.contains(colDef.columnName))
          )
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
        } else {
          // Storage format is TSV.
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

}

class ColumnFilterRuntime extends SparkRuntimeWithIOTypedJob[
  ColumnFilterJob,
  HdfsTabularDataset,
  HdfsTabularDataset] {}

class ColumnFilterJob extends
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
    val schemaOutline = input.getTabularSchema()
    println("Input schema : ")
    schemaOutline.getDefinedColumns().foreach(
      colDef =>
        println(colDef.columnName + " : " + colDef.columnType.name)
    )
    val dataFrame = sparkUtils.getDataFrame(
      input
    )

    listener.notifyMessage("Starting the feature transformer.")

    val (_, columnsToKeep) =
      operatorParameters.getTabularDatasetSelectedColumns("columnsToKeep")

    listener.notifyMessage("COlumns to keep are : " + columnsToKeep.mkString(","))

    val outputPathStr = operatorParameters.getStringValue("outputPath")

    listener.notifyMessage("Output path is : " + outputPathStr)

    val storageFormat = operatorParameters.getStringValue("storageFormat")
    val columnsToLeave = dataFrame.columns.filter(c => columnsToKeep.contains(c)).map(dataFrame.col)
    val transformedDataFrame =
      dataFrame.select(columnsToLeave: _*)

    val outputPath = new Path(outputPathStr)
    val driverHdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    if (driverHdfs.exists(outputPath)) {
      driverHdfs.delete(outputPath, true)
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
      } else {
        // Storage format is TSV.
        sparkUtils.saveAsTSV(
          outputPathStr,
          transformedDataFrame
        )
      }

    output
  }
}
