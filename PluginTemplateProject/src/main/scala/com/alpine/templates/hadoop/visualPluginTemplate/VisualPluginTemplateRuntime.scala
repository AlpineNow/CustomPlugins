package com.alpine.templates.hadoop.visualPluginTemplate

import com.alpine.plugin.core.io.defaults.IONoneDefault
import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.core.io.{HdfsTabularDataset, IONone}
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkRuntime}
import com.alpine.plugin.core.visualization.{JavascriptVisualModel, TabularVisualModel, VisualModel}

class VisualPluginTemplateRuntime extends SparkRuntime[HdfsTabularDataset, IONone] {
  import VisualPluginTemplateUtils._
  /**
   * The point of this example custom operator. In a very trivial exercise, we produce a subset
   * of the input columns as specified by x and y values specified by user. These values are then
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

    val dataSet = context.visualModelHelper.createTabularDatasetVisualization(input, rowsToFetch = 5000)
    val tabularModel = dataSet.asInstanceOf[TabularVisualModel]

    val srcIdCol = params.getTabularDatasetSelectedColumn(SRC_ID)._2
    val srcIdColNum = tabularModel.columnDefs.indexWhere(columnDef => columnDef.columnName == srcIdCol)
    val srcLabelCol = params.getTabularDatasetSelectedColumn(SRC_LABEL)._2
    val srcLabelColNum = tabularModel.columnDefs.indexWhere(columnDef => columnDef.columnName == srcLabelCol)

    val valuesSet = tabularModel.content.map(row => Tuple2(row(srcIdColNum), row(srcLabelColNum)))

    JavascriptVisualModel("execute", Option.apply(valuesSet))
  }

  // Visual operators return IONone from onExecution
  override def onExecution(context: SparkExecutionContext,
                           input: HdfsTabularDataset,
                           params: OperatorParameters,
                           listener: OperatorListener): IONone = {

    new IONoneDefault(Map[String, AnyRef]())
  }

  // SparkRuntime override
  override def onStop(context: SparkExecutionContext,
                      listener: OperatorListener): Unit = {}

}

