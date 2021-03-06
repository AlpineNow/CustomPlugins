package com.alpine.plugin.samples.advanced

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

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog, SparkParameter}
import com.alpine.plugin.core.io.{ColumnDef, HdfsTabularDataset, OperatorSchemaManager, TabularSchema}
import com.alpine.plugin.core.spark.{AutoTunerOptions, SparkExecutionContext}
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.SparkParameterUtils
import com.alpine.plugin.core.visualization._
import org.apache.spark.sql.DataFrame

/**
  * The column filter plugin with the addition of some advanced features
  */
class AdvancedColumnFilterSignature extends OperatorSignature[
  AdvancedColumnFilterGUINode,
  AdvancedColumnFilterRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Spark Column Filter (Advanced)",
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

/**
  * Util with the constants for the plugin that will be used in multiple classes.
  */
object ColumnFilterUtil {
  /**
    * Key for the column selection parameter.
    */
  val COLUMNS_TO_KEEP_KEY = "columnsToKeep"

  val MESSAGE_STRING_KEY = "message"
  val HTML_MESSAGE_KEY = "htmlMessage"

  /**
    * Returns the value of the columnsToKeep parameter, a list of the columns which will be in
    * the output.
    */
  def getColumnsToKeep(parameters: OperatorParameters): Seq[String] = {
    parameters.getTabularDatasetSelectedColumns(ColumnFilterUtil.COLUMNS_TO_KEEP_KEY)._2
  }
}

/**
  * The ColumnFilterGUINode defines the design time behavior of the plugin. It extends
  * the template GUI class "SparkDataFrameGUINode" which takes the spark job as a type parameter.
  * It is in this class that we define the parameters which the user will see when they click on the
  * operator and the design time output schema.
  */
class AdvancedColumnFilterGUINode extends SparkDataFrameGUINode[AdvancedColumnFilterJob] {
  /**
    * We are now adding the advanced Spark parameters
    */
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnCheckboxes(
      ColumnFilterUtil.COLUMNS_TO_KEEP_KEY, //key so we can access this parameter in the Spark job
      "Columns To Keep", //display name of the parameter
      ColumnFilter.All, //A filter which specifies what types of columns can be selected
      "main" // This is a string used to group together column selectors.
    )

    /*
    Call the super method which adds the default storage parameters. See documentation for the
    HdfsParameterUtils class and the SparkDataFrameGUINode for more information.
     */

    SparkParameterUtils.addStandardSparkOptions(
      operatorDialog,
      List[SparkParameter](SparkParameterUtils.makeStorageLevelParam("NONE"),
        SparkParameterUtils.makeRepartitionParam, SparkParameterUtils.makeNumPartitionsParam)
    )

    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

  /**
    * A method provided by the template which lets us define the output Tabular schema by
    * specifying the columns that the output schema will include. By using this method, we are
    * defining a schema which has the type (CSV, Avro, Parquet) that the user selected. In this
    * case we define the output schema as containing only the columns that the user selected in
    * the "Columns to keep" parameter.
    *
    * See the documentation of the SparkDataFrameGUINode for more information.
    */
  override def defineOutputSchemaColumns(inputSchema: TabularSchema,
                                         parameters: OperatorParameters): Seq[ColumnDef] = {
    //get the value of the columns to keep parameter using the util. This is a list of names.
    val columnsToKeep = ColumnFilterUtil.getColumnsToKeep(parameters).toSet

    /* Filter the list of Column Definitions in the input schema to include only those columns
    whose name is in the columnsToKeep set.
    */
    inputSchema.getDefinedColumns.filter(colDef => columnsToKeep.contains(colDef.columnName))
  }

  /**
    * Exercise 3: Requiring that more than two columns are selected to keep
    *
    * @group internals
    */
  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    this.updateOutputSchema(
      inputSchemas,
      params,
      operatorSchemaManager
    )
    val (_, colsSelected) = params.getTabularDatasetSelectedColumns(
      ColumnFilterUtil.COLUMNS_TO_KEEP_KEY)
    if (colsSelected.length < 2) {
      OperatorStatus(isValid = false, Some("You need to select at least two columns"))
    }
    else OperatorStatus(isValid = true)
  }

}

/**
  * What happens when the alpine user clicks the "run button". In this case the base class,
  * SparkDataFrameRuntime, handles launching the spark job and serializing/de-serializing the inputs
  * The class takes one type parameter: ColumnFilterJob, which extends SparkDataFrameJob and
  * defines the Spark Job.
  */
class AdvancedColumnFilterRuntime extends SparkDataFrameRuntime[AdvancedColumnFilterJob] {
  /**
    * Set the options passed to our Spark Auto Tuner which will choose optimal Spark configuration
    * settings for values not provided by the user based on the size of the cluster, the input
    * data and the type of computation.
    * See documentation for the AutoTunerOptions object for more details on what the settings in this
    * object mean.
    * Set only the auto tuning options by overriding this method.
    * To change the parameters passed the Spark Configuration more comprehensively override
    * 'getSparkJobConfiguration' and this method will be ignored.
    */
  override def getAutoTuningOptions(parameters: OperatorParameters, input: HdfsTabularDataset): AutoTunerOptions = {
    AutoTunerOptions(
      driverMemoryFraction = 0.5,
      inputCachedSizeMultiplier = 1.0,
      minExecutorMemory = 1800L)
  }

  /**
    * This is called to generate the visual output for the results console.
    * If the developer does not override it, we try OperatorGUINode#onOutputVisualization,
    * which predated this, so we keep for compatibility.
    *
    * @param context  Execution context of the operator.
    * @param input    The input to the operator.
    * @param output   The output from the execution.
    * @param params   The parameter values to the operator.
    * @param listener The listener object to communicate information back to
    *                 the console.
    * @return
    */
  override def createVisualResults(
      context: SparkExecutionContext,
      input: HdfsTabularDataset,
      output: HdfsTabularDataset,
      params: OperatorParameters,
      listener: OperatorListener
  ): VisualModel = {
    //create the standard visualization of the output data
    val datasetVisualModel = context.visualModelHelper.createTabularDatasetVisualization(output)
    val addendum: Map[String, AnyRef] = output.addendum
    val addendumVisualModel =
      TextVisualModel(
        /**
          * Get the key from the addendum
          * We have to get it, since get returns an option type and convert to String since
          * it is of type AnyRef
          */
        addendum(ColumnFilterUtil.MESSAGE_STRING_KEY).toString)

    val htmlVisualModel = HtmlVisualModel(addendum(ColumnFilterUtil.HTML_MESSAGE_KEY).toString)
    val compositeVisualModel = new CompositeVisualModel()
    compositeVisualModel.addVisualModel("Dataset", datasetVisualModel)
    compositeVisualModel.addVisualModel("MessageString", addendumVisualModel)
    compositeVisualModel.addVisualModel("Columns Selected", htmlVisualModel)
    compositeVisualModel
  }
}

/**
  * The logic of the SparkJob launched in the runtime class. Since we are using the template,
  * and don't need any custom visualization the only part of the Spark job we have to define
  * is the DataFrame transformation as the template handles all the reading, saving, and parsing.
  */
class AdvancedColumnFilterJob extends SparkDataFrameJob {

  override def transformWithAddendum(parameters: OperatorParameters,
                                     dataFrame: DataFrame,
                                     sparkUtils: SparkRuntimeUtils,
                                     listener: OperatorListener): (DataFrame, Map[String, AnyRef]) = {

    val repartition = SparkParameterUtils.getRepartition(parameters)
    listener.notifyMessage("Repartition:  " + repartition)
    val defaultParallelism = dataFrame.rdd.sparkContext.getConf.get("spark.default.parallelism", "1")
    val numPartitions = SparkParameterUtils.getUserSetNumPartitions(parameters) match {
      case Some(p) => Math.max(p, defaultParallelism.toInt)
      case None => defaultParallelism.toInt
    }

    listener.notifyMessage(" The number of partitions set to use is " + numPartitions)
    val partitioned: DataFrame = if (repartition) {
      dataFrame.sqlContext.createDataFrame(dataFrame.rdd.repartition(numPartitions), dataFrame.schema)
    } else {
      dataFrame
    }
    //get the value of the columnsToKeep parameter
    val columnNamesToKeep = ColumnFilterUtil.getColumnsToKeep(parameters)
    // map the list of column names to DataFrame column definitions.
    val columnsToKeep = columnNamesToKeep.map(name => dataFrame.col(name))
    // Use the select function on the DataFrame to select all the columns to keep.
    /**
      * Exercise 3: Add addendum with the number of rows in the output
      */
    val messageString = "Number of rows in the output " + partitioned.count()

    /**
      * Exercise 4: Add a visualization which lists the parameters and bolds the header to
      * the output
      */
    val htmlMessageString = "<b>Columns Selected </b> <br>" + columnNamesToKeep.mkString("<br>")
    (partitioned.select(columnsToKeep: _*),
      Map(ColumnFilterUtil.MESSAGE_STRING_KEY -> messageString,
        ColumnFilterUtil.HTML_MESSAGE_KEY -> htmlMessageString))
  }

}
