package com.alpine.plugin.samples.advanced

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, OperatorSchemaManager, TabularSchema}
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.AddendumWriter
import com.alpine.plugin.core.{OperatorListener, OperatorMetadata, OperatorParameters, OperatorSignature}
import org.apache.spark.sql.DataFrame

object DateTypeTranslatorUtils {
  val dateColumnParamId = "dateCol"
  val dateFormatParamId = "dateFormat"

  def outputSchema(parameters: OperatorParameters,
                   inputSchema: TabularSchema) = {

    val (_, dateColumn) = parameters.getTabularDatasetSelectedColumn(DateTypeTranslatorUtils.dateColumnParamId)

    val dateFormat =
      parameters.getStringValue(DateTypeTranslatorUtils.dateFormatParamId)

    TabularSchema(inputSchema.getDefinedColumns ++ Seq(ColumnDef(dateColumn + "_formatted",
      ColumnType.DateTime(dateFormat))))
  }
}

class DateTypeTranslatorSignature extends OperatorSignature[DateTypeTranslatorGUINode, DateTypeTranslatorRuntime] {
  /**
    * This should be implemented by every operator to provide metadata
    * about the operator itself.
    * @return Metadata about this operator. E.g. the version, the author, the
    *         category, etc.
    */
  override def getMetadata(): OperatorMetadata = new OperatorMetadata(
    name = "Sample - Date Type Translator",
    category = "Sample - Transformations", 1)
}

class DateTypeTranslatorGUINode extends SparkDataFrameGUINode[DateTypeTranslatorJob] {

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnDropdownBox(DateTypeTranslatorUtils.dateColumnParamId,
      "Date Column change", ColumnFilter.All, "main")

    operatorDialog.addStringBox(DateTypeTranslatorUtils.dateFormatParamId, "Date Format String",
      "MM/dd/yyyy", ".+", 0, 0)

    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }
}

class DateTypeTranslatorRuntime extends SparkDataFrameRuntime[DateTypeTranslatorJob] {}

class DateTypeTranslatorJob extends SparkDataFrameJob {
  override def transformWithAddendum(parameters: OperatorParameters,
                                     dataFrame: DataFrame,
                                     sparkUtils: SparkRuntimeUtils,
                                     listener: OperatorListener): (DataFrame, Map[String, AnyRef]) = {
    //get the value of the columnsToKeep parameter
    val (_, dateColumn) = parameters.getTabularDatasetSelectedColumn(DateTypeTranslatorUtils.dateColumnParamId)
    val dateFormat = parameters.getStringValue(DateTypeTranslatorUtils.dateFormatParamId)

    val finalData = dataFrame.withColumn(dateColumn + "_formatted",
      dataFrame(dateColumn))

    val finalRows = finalData.rdd
    val inputSchema = sparkUtils.convertSparkSQLSchemaToTabularSchema(dataFrame.schema)

    val newSchema = sparkUtils.convertTabularSchemaToSparkSQLSchema(DateTypeTranslatorUtils.outputSchema(parameters, inputSchema))
    val resultDF = dataFrame.sqlContext.createDataFrame(finalRows, newSchema)

    (resultDF, AddendumWriter.createStandardAddendum("Converted the column "
      + dateColumn + " to the date format " + dateFormat))
  }

}
