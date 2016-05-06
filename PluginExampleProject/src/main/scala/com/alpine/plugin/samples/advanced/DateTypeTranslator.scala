package com.alpine.plugin.samples.advanced

import java.text.SimpleDateFormat

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, OperatorSchemaManager, TabularSchema}
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.AddendumWriter
import org.apache.spark.sql.DataFrame

import scala.util.Try

object DateTypeTranslatorUtils {
  val dateColumnParamId = "dateCol"
  val dateFormatParamId = "dateFormat"

  def outputSchema(parameters: OperatorParameters,
                   inputSchema: TabularSchema) = {

    val (_, dateColumn) = parameters.getTabularDatasetSelectedColumn(DateTypeTranslatorUtils.dateColumnParamId)

    val dateFormat =
      parameters.getStringValue(DateTypeTranslatorUtils.dateFormatParamId)

    //the new schema is the same but with one new column that has a different format for the DateType
    TabularSchema(inputSchema.getDefinedColumns ++ Seq(ColumnDef(dateColumn + "_formatted",
      ColumnType.DateTime(dateFormat))))
  }
}

class DateTypeTranslatorSignature extends OperatorSignature[DateTypeTranslatorGUINode, DateTypeTranslatorRuntime] {

  override def getMetadata(): OperatorMetadata = new OperatorMetadata(
    name = "Sample - Date Type Translator",
    category = "Plugin Sample - Spark", 1)
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

  /**
    * We have write custom validation code to check that the columns selected are date columns and that the
    * date format string is a valid SimpleDateFormat.
    */
  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager):
  OperatorStatus = {

    val (_, selectedColumn) = params.getTabularDatasetSelectedColumn(DateTypeTranslatorUtils.dateColumnParamId)

    //will only be evaluated is there is an input schema
    lazy val dateColumn = inputSchemas.head._2.getDefinedColumns.find(_.columnName == selectedColumn)

    val format: String = params.getStringValue(DateTypeTranslatorUtils.dateFormatParamId)

    if (inputSchemas.nonEmpty && dateColumn.isDefined && !isDateColumn(dateColumn.get)) {
      OperatorStatus(isValid = false, "The column selected must be have type 'DateTime'")
    }
    else if (Try(new SimpleDateFormat(format)).isFailure) {
      OperatorStatus(isValid = false, format + " is not a valid SimpleDateFormat")
    }
    else {
      super.onInputOrParameterChange(inputSchemas, params, operatorSchemaManager)
    }
  }

  def isDateColumn(columnDef: ColumnDef): Boolean = {
    columnDef.columnType.name == ColumnType.DateTime.name
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
    //we don't have to change the data frame to change the type of the date. All we need to do is assign it a different schema which specifies the date as a different type.
    //then the framework will handel the conversion for us.
    val resultDF = dataFrame.sqlContext.createDataFrame(finalRows, newSchema)

    (resultDF, AddendumWriter.createStandardAddendum("Converted the column "
      + dateColumn + " to the date format " + dateFormat))
  }

}
