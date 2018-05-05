package com.alpine.plugin.samples.advanced

/*
 * Copyright (c) 2017 Alpine Data Labs
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
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.defaults.IONoneDefault
import com.alpine.plugin.core.io.{IONone, HdfsTabularDataset, OperatorSchemaManager}
import com.alpine.plugin.core.spark.{SparkRuntime, SparkExecutionContext}
import com.alpine.plugin.core.visualization._

import com.google.gson.Gson

/**
  * A demonstration of using Javascript in operator output
  */
class JavascriptVisualizationSignature extends OperatorSignature[
  JavascriptVisualizationGUINode,
  JavascriptVisualizationRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - Javascript Visualization",
    category = "Plugin Sample - Spark",
    author = Some("Alpine Data"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words."),
    usesJavascript = true
  )
}

object JavascriptVisualizationUtil {
  /**
    * Key for the column selection parameter.
    */
  val COLUMN_TO_ANALYZE = "columnToAnalyze"
}

/**
  * The JavascriptVisualizationGUINode defines the design time behavior of the plugin. It extends
  * the template GUI class "OperatorGUINode".
  * It is in this class that we define the parameters which the user will see when they click on the
  * operator and the design time output schema.
  */
class JavascriptVisualizationGUINode extends OperatorGUINode[HdfsTabularDataset, IONone] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnDropdownBox(JavascriptVisualizationUtil.COLUMN_TO_ANALYZE,
      "Column", ColumnFilter.All, "main")
  }
}

/**
  * What happens when the alpine user clicks the "run button".
  */
class JavascriptVisualizationRuntime extends SparkRuntime[HdfsTabularDataset, IONone] {

  /**
    * The point of this example custom operator. In a very trivial exercise, we find unique values for the specified
    * column from a subset (as created by createTabularDatasetVisualization()) of the input. These values are then
    * converted to a Json array and passed to the JavascriptVisualModel.
    *
    * @param context  Execution context of the operator.
    * @param input    The input to the operator.
    * @param output   The output from the execution.
    * @param params   The parameter values to the operator.
    * @param listener The listener object to communicate information back to
    *                 the console.
    * @return VisualModel
    */
  override def createVisualResults(context: SparkExecutionContext,
                                   input: HdfsTabularDataset,
                                   output: IONone,
                                   params: OperatorParameters,
                                   listener: OperatorListener): VisualModel = {
    val dataset = context.visualModelHelper.createTabularDatasetVisualization(input)
    val tabularModel = dataset.asInstanceOf[TabularVisualModel]

    val columnName = params.getTabularDatasetSelectedColumn(JavascriptVisualizationUtil.COLUMN_TO_ANALYZE)._2
    val i = tabularModel.columnDefs.indexWhere(columnDef => columnDef.columnName == columnName)

    val compositeVisualModel = new CompositeVisualModel()
    if (i != -1) {
      val valuesSet = tabularModel.content.map(row => row(i))
      compositeVisualModel.addVisualModel("Data Cloud",
        JavascriptVisualModel("execute", Option.apply(valuesSet)))
    } else {
      compositeVisualModel.addVisualModel("Data Cloud",
        JavascriptVisualModel("execute", Option.apply(new Gson().toJson(Array("No results")))))
    }
    compositeVisualModel.addVisualModel("Hello World",
      JavascriptVisualModel("simpleFunction", Option.apply()))
    compositeVisualModel
  }

  override def onExecution(context: SparkExecutionContext, input: HdfsTabularDataset, params: OperatorParameters,
                           listener: OperatorListener): IONone = {
    new IONoneDefault(Map[String, AnyRef]())
  }

  override def onStop(context: SparkExecutionContext, listener: OperatorListener): Unit = {}
}
