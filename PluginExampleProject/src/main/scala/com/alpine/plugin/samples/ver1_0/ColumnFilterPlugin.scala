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

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import org.apache.spark.sql.{Column, DataFrame}

/**
  * An example of how to write a standard Column Filter plugin to run on Spark
  * using our plugin framework.
  *
  * This plugin uses our DataFrame template, which can be used for any Plugin that takes an
  * HDFSTabularDataset as input and returns an HDFSTabularDataset. The template removes much of the
  * boiler plate code involved in the plugin process. We handle the conversion of the input
  * HDFSTabularDataset into a SparkDataFrame object and from the Spark DataFrame to the output
  * HDFSTabularDataset. That way the user can define their spark job as a DataFrame transformation by
  * overriding the "transform" method in the spark job class which takes a DataFrame as input and
  * returns a DataFrame.
  */

/**
  * The Signature class of Plugin. This defines how the plugin will appear both in the list of
  * jars and plugins in the "Manage Custom Operators" tab and in the Alpine GUI.
  * The class takes two type parameters:
  * - ColumnFilterGUINode: extends SparkDataFrameGUINode (which itself extends OperatorGUINode) and
  * defines the design time behavior of the plugin (including the parameter definitions, how the
  * output will be visualized, and the design time output schema).
  * - SimpleDatasetGeneratorRuntime: extends SparkDataFrameRuntime (which itself extends
  * SparkRuntimeWithIOTypedJob) and launches the Spark job, defined in SimpleDatasetGeneratorJob.
  */
class ColumnFilterSignature extends OperatorSignature[
  ColumnFilterGUINode,
  ColumnFilterRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Spark Column Filter",
    category = "Plugin Sample - Spark",
    author = Some("Rachel Warren"),
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
class ColumnFilterGUINode extends SparkDataFrameGUINode[ColumnFilterJob] {
  /**
    * * Defines the behavior of the plugin when the user drags the plugin from the side bar into the
    * workflow canvas. In particular, defines the all the elements in the parameter dialog box which
    * appears when you click on the plugin. In this case we add column selector box so that user can
    * check all of the columns that they want to keep and some parameters so the use can define how
    * they want the result data set to be stored.
    *
    * @param operatorDialog            The operator dialog where the operator could add
    *                                  input text boxes, etc. to define UI for parameter
    *                                  inputs.
    * @param operatorDataSourceManager Before executing the runtime of the operator
    *                                  the developer should determine the underlying
    *                                  platform that the runtime will execute against.
    *                                  E.g., it is possible for an operator to have
    *                                  accesses to two different Hadoop clusters
    *                                  or multiple databases. A runtime can run
    *                                  on only one platform. A default platform
    *                                  will be used if nothing is done.
    * @param operatorSchemaManager     This can be used to provide information about
    *                                  the nature of the output/input schemas.
    *                                  E.g., provide the output schema.
    */
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnCheckboxes(
      ColumnFilterUtil.COLUMNS_TO_KEEP_KEY, //key so we can access this parameter in the Spark job
      "Columns to keep", //display name of the parameter
      ColumnFilter.All, //A filter which specifies what types of columns can be selected
      "main" // This is a string used to group together column selectors.
      // If there is only one column selector, this string can be anything.
      // If there are multiple column selectors, then validation will require any set of
      // column selectors with the same group id to have disjoint columns selected.
    )
    /*
    Call the super method which adds the default storage parameters. See documentation for the
    HdfsParameterUtils class and the SparkDataFrameGUINode for more information.
     */
    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

  /**
    * A method provided by the template which lets us define the output Tabular schema by
    * specifying the columns that the output schema will include. By using this method, we are
    * defining a schema which has the type (TSV, Avro, Parquet) that the user selected. In this
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
}

/**
  * What happens when the alpine user clicks the "run button". In this case the base class,
  * SparkDataFrameRuntime, handles launching the spark job and serializing/de-serializing the inputs
  * The class takes one type parameter: ColumnFilterJob, which extends SparkDataFrameJob and
  * defines the Spark Job.
  */
class ColumnFilterRuntime extends SparkDataFrameRuntime[ColumnFilterJob] {}

/**
  * The logic of the SparkJob launched in the runtime class. Since we are using the template,
  * and don't need any custom visualization the only part of the Spark job we have to define
  * is the DataFrame transformation as the template handles all the reading, saving, and parsing.
  */
class ColumnFilterJob extends SparkDataFrameJob {
  override def transform(parameters: OperatorParameters,
                         dataFrame: DataFrame,
                         sparkUtils: SparkRuntimeUtils,
                         listener: OperatorListener): DataFrame = {
    //get the value of the columnsToKeep parameter
    val columnNamesToKeep: Seq[String] = ColumnFilterUtil.getColumnsToKeep(parameters)
    // map the list of column names to DataFrame column definitions.
    val columnsToKeep: Seq[Column] = columnNamesToKeep.map(name => dataFrame.col(name))
    // Use the select function on the DataFrame to select all the columns to keep.
    dataFrame.select(columnsToKeep: _*)
  }
}
