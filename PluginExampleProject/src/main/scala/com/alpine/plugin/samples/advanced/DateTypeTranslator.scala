package com.alpine.plugin.samples.advanced

import java.text.SimpleDateFormat

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.SparkExecutionContext
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.AddendumWriter
import com.alpine.plugin.core.visualization.VisualModel
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * This operator is intended to demonstrate how custom date formats are handled in the Custom Operator Framework.
  * It allows the user to select a column and write in a Java Simple Date Format. It will convert
  * the selected column to the new date format.
  */
object DateTypeTranslatorUtils {
  val dateColumnParamId = "dateCol"
  val dateFormatParamId = "dateFormat"

  def outputSchema(parameters: OperatorParameters,
                   inputSchema: TabularSchema): TabularSchema = {

    val (_, dateColumn) = parameters.getTabularDatasetSelectedColumn(DateTypeTranslatorUtils.dateColumnParamId)

    val dateFormat =
      parameters.getStringValue(DateTypeTranslatorUtils.dateFormatParamId)

    //the new schema is the same but with one new column that has a different format for the DateType
    TabularSchema(inputSchema.getDefinedColumns ++ Seq(ColumnDef(dateColumn + "_formatted",
      ColumnType.DateTime(dateFormat))))
  }
}

class DateTypeTranslatorSignature extends OperatorSignature[DateTypeTranslatorGUINode, DateTypeTranslatorRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Date Type Translator",
    category = "Plugin Sample - Spark",
    author = Some("Alpine Data"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words.")
  )
}

class DateTypeTranslatorGUINode extends SparkDataFrameGUINode[DateTypeTranslatorJob] {

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnDropdownBox(DateTypeTranslatorUtils.dateColumnParamId,
      "Date Column change", ColumnFilter.DateTimeOnly, "main")

    operatorDialog.addStringBox(DateTypeTranslatorUtils.dateFormatParamId, "Date Format String",
      "MM/dd/yyyy", ".+", required = true)

    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

  /**
    * We have to write custom validation code to check that the
    * date format string is a valid SimpleDateFormat.
    *
    * A Date Time Column Filter will be made available in the next release of the SDK.
    */
  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager):
  OperatorStatus = {

    val format: String = params.getStringValue(DateTypeTranslatorUtils.dateFormatParamId)
    if(Try(new SimpleDateFormat(format)).isFailure) {
      OperatorStatus(isValid = false, format + " is not a valid SimpleDateFormat")
    }
    else {
      super.onInputOrParameterChange(inputSchemas, params, operatorSchemaManager)
    }
  }

}

class DateTypeTranslatorRuntime extends SparkDataFrameRuntime[DateTypeTranslatorJob] {

  /**
    * Overwrite the createVisualResults method to get a visualization with two tabs one for output and one for
    * Summary like other Alpine Transformation operators such as T-Test and Regression Evaluator.
    */
  override def createVisualResults(context: SparkExecutionContext,
                                   input: HdfsTabularDataset,
                                   output: HdfsTabularDataset,
                                   params: OperatorParameters,
                                   listener: OperatorListener): VisualModel =
    AddendumWriter.createCompositeVisualModel(context.visualModelHelper, output, Seq())
}

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

    val newSchema =
      sparkUtils.convertTabularSchemaToSparkSQLSchema(DateTypeTranslatorUtils.outputSchema(parameters, inputSchema))
    //we don't have to change the data frame to change the type of the date.
    // All we need to do is assign it a different schema which specifies the date as a different type.
    //then the framework will handel the conversion for us.
    val resultDF = dataFrame.sqlContext.createDataFrame(finalRows, newSchema)

    //the "createStandardAddendum" method is designed to be used in tandem with the "

    (resultDF, AddendumWriter.createStandardAddendum("Converted the column "
      + dateColumn + " to the date format " + dateFormat))
  }

}
