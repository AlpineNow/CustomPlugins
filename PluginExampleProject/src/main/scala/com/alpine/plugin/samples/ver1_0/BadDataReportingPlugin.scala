package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io.{HdfsTabularDataset, OperatorSchemaManager}
import com.alpine.plugin.core.spark.SparkExecutionContext
import com.alpine.plugin.core.spark.reporting.NullDataReportingUtils
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.{HdfsParameterUtils, HtmlTabulator, SparkParameterUtils, Timer}
import com.alpine.plugin.core.visualization._
import com.alpine.plugin.core.{OperatorListener, OperatorMetadata, OperatorParameters, OperatorSignature}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

/**
  * A Custom Operator which demonstrates how to use our built in features for filtering and reporting
  * bad data, and displaying html formatted results.
  * The Custom Operator takes in some data, filters out the bad data, and prints the bad
  * data to a file. Has two parameters besides the default storage parameters:
  * 1) weather to write null data to a file or not (uses the parameter defined in Hdfs tabular dataset to
  * do this
  * 2) A custom style tag to use in the "fancy" html table.
  * Additionally the Custom operator creates a visualization of three items:
  * 1) The good data
  * 2) An html table (with default styling) of a report about the bad data which was removed
  * 3) A "fancy" html table with custom styling determined by the value of the input parameter
  */
class BadDataReportingPluginSignature extends OperatorSignature[
  BadDataReportingPluginGUINode,
  BadDataReportingPluginRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Bad Data Reporting",
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

object BadDataConstants {
  val badDataTypeParamId = "badType"

  val ALL_NULL = "All rows with null values"
  val NULL_AND_ZERO = "Rows with zeros in numeric columns or null values"
  val badDataOptions = Seq(ALL_NULL, NULL_AND_ZERO)

  val styleTagParamId = "styleTag"
  val fancyHtmlTableId = "fancy"
  val badDataReportId = "bad"
  val timerReportId = "timer"

  val DEFAULT_STYLE_TAG = "padding-right:50px;"
}

class BadDataReportingPluginGUINode extends SparkDataFrameGUINode[BadDataReportingPluginJob] {

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    //parameter for filtering out just nulls or nulls and zeros.
    operatorDialog.addDropdownBox(id = BadDataConstants.badDataTypeParamId, label = "Filter out",
      values = BadDataConstants.badDataOptions, defaultSelection = BadDataConstants.ALL_NULL)

    HdfsParameterUtils.addNullDataReportParameter(operatorDialog)

    //a custom style tag to be used in formatting the "fancy table"
    operatorDialog.addStringBox(id = BadDataConstants.styleTagParamId, label = "Custom Style Tag",
      defaultValue = BadDataConstants.DEFAULT_STYLE_TAG, regex = ".+", required = true)

    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)

    SparkParameterUtils.addStandardSparkOptionsWithStorageLevel(
      operatorDialog = operatorDialog,
      defaultNumExecutors = 2,
      defaultExecutorMemoryMB = 1024,
      defaultDriverMemoryMB = 1024,
      defaultNumExecutorCores = 5,
      defaultStorageLevel = "MEMORY_AND_DISK")
  }

}

class BadDataReportingPluginRuntime extends SparkDataFrameRuntime[BadDataReportingPluginJob] {

  override def createVisualResults(
    context: SparkExecutionContext,
    input: HdfsTabularDataset,
    output: HdfsTabularDataset,
    params: OperatorParameters,
    listener: OperatorListener): VisualModel = {
    val addendum = output.addendum
    val fancyHtmlTable = addendum.getOrElse(BadDataConstants.fancyHtmlTableId, "").toString
    val badDataReport = addendum.getOrElse(BadDataConstants.badDataReportId, "").toString
    val timerTable = addendum.getOrElse(BadDataConstants.timerReportId, "").toString
    val compositeVisualModel =new CompositeVisualModel()
    compositeVisualModel.addVisualModel("Good Data", context.visualModelHelper.createTabularDatasetVisualization(output))
    compositeVisualModel.addVisualModel("Bad Data Report", HtmlVisualModel(badDataReport))
    compositeVisualModel.addVisualModel("Test of Fancy Html Table", HtmlVisualModel(fancyHtmlTable))
    compositeVisualModel.addVisualModel("Timer Report", HtmlVisualModel(timerTable))
    compositeVisualModel
  }
}


class BadDataReportingPluginJob extends SparkDataFrameJob {

  override def transformWithAddendum(operatorParameters: OperatorParameters,
                                     dataFrame: DataFrame, sparkUtils: SparkRuntimeUtils,
                                     listener: OperatorListener):
  (DataFrame, Map[String, AnyRef]) = {
    var timer = new Timer()
    timer = timer.start
    val typeOfBadDataToRemove = operatorParameters.getStringValue(BadDataConstants.badDataTypeParamId)
    listener.notifyMessage("param = " + typeOfBadDataToRemove)

    //Retrieve the storage level parameter set with the utils
    //Note: The other Spark parameters will be automatically configured when the job is launched
    val storageLevel = StorageLevel.fromString(
      SparkParameterUtils.getStorageLevel(operatorParameters).getOrElse("MEMORY_AND_DISK")) //get the user defined storage level

    //The bad data reporting utils have to call an action on the data in order to count the removed rows.
    //To avoid recomputing the expensive read step, it may be faster to persist the data.
    dataFrame.persist(storageLevel)

    //use the BadDataReportingUtils to filter out the rows with null/zero data
    //This function takes a custom routine called that maps from a row to a boolean.
    //In this case we pass in a function (defined bellow) called 'containsZero' that labels as removable if they contain any zeros.
    val nullDataStrategy = HdfsParameterUtils.getNullDataStrategy(operatorParameters, None)
    val (goodData, badDataReport) =
      if (typeOfBadDataToRemove.equals(BadDataConstants.NULL_AND_ZERO)) {
        val head: Column = RowProcessingUtil.isNullNanZero(dataFrame.col(dataFrame.columns.head))
        val containsZeros: Column =
          if(dataFrame.columns.length > 1) {
            dataFrame.columns.tail.foldLeft(head)(
              (acc, colName) => acc.or(RowProcessingUtil.isNullNanZero(dataFrame.col(colName))))
          } else
            head
        NullDataReportingUtils.filterNullDataAndReportGeneral(
          dataFrame, sparkUtils, nullDataStrategy, containsZeros,
          dataRemovedDueTo = "due to nulls and zeros") //the report will say "rows removed 'due to nulls and zeros'
      }
      //the default bad data function is .anyNull in sql.Row
      else NullDataReportingUtils.filterNullDataAndReport(dataFrame, sparkUtils, nullDataStrategy)

    timer = timer.stop

    val styleTag = operatorParameters.getStringValue(BadDataConstants.styleTagParamId)

    val parameterTable = Seq(
      Seq("Parameter", "Selected Value"),
      Seq("Type Of Data To remove: ", typeOfBadDataToRemove),
      Seq("StyleTag", styleTag))
    val timerTable = HtmlTabulator.format(timer.report())
    val fancyTable = HtmlTabulator.format(parameterTable, styleTag)

    val addendum = Map[String, AnyRef](
      BadDataConstants.badDataReportId -> badDataReport,
      BadDataConstants.fancyHtmlTableId -> fancyTable,
      BadDataConstants.timerReportId -> timerTable)
    (goodData, addendum)
  }
}

/**
  * When defining sub routines to be performed by spark transformations it is best to put them in a
  * static object extending the "Serializable" Trait to avoid serialization errors.
  * The important thing is that nothing in these functions relies on mutable values that could be changed
  * during the Spark job.
  *
  *
  * These routines are used for the 'removeNullData' function.
  */
object RowProcessingUtil extends Serializable {
  /**
    * This is a function that maps from a row to a boolean and is passed to the
    * 'removeNullData' routine.
    * It is true if any value is not numeric or is a zero.
    */
  def isNullNanZero(col : Column) : Column = col.isNull.or(col.isNaN).or(col.eqNullSafe(0))

}