/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.utils.OutputParameterUtils
import com.alpine.plugin.model.RegressionModelWrapper

/**
 * This is the design-time code for the Regression evaluator operator.
 * It takes an input dataset and a Regression model to create a set of metrics
 * evaluating the quality of the model on the dataset.
 *
 * The result is a dataset containing one row, where each statistic is in a separate column.
 */
class RegressionEvaluatorSignature extends OperatorSignature[
  RegressionEvaluatorGUINode,
  RegressionEvaluatorRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Regression Evaluator",
      category = "Model Evaluator",
      author = "Jenny Thompson",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class RegressionEvaluatorGUINode extends OperatorGUINode[
  Tuple2[HdfsTabularDataset, RegressionModelWrapper],
  HdfsTabularDataset
  ] {

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    OutputParameterUtils
          .addStandardHDFSOutputParameters(operatorDialog, operatorDataSourceManager)

    val numColumns = 4
    val outputSchema: TabularSchemaOutline = operatorSchemaManager.createTabularSchemaOutline(
      minNumCols = numColumns,
      maxNumCols = numColumns
    )
    outputSchema.addColumnDef(new ColumnDef("explainedVariance", ColumnType.Double))
    outputSchema.addColumnDef(new ColumnDef("meanAbsoluteError", ColumnType.Double))
    outputSchema.addColumnDef(new ColumnDef("meanSquaredError", ColumnType.Double))
    outputSchema.addColumnDef(new ColumnDef("rootMeanSquaredError", ColumnType.Double))
    outputSchema.addColumnDef(new ColumnDef("r2", ColumnType.Double))
    operatorSchemaManager.setOutputSchemaOutline(outputSchema)

    outputSchema.setExpectedOutputFormat(
      TabularFormatAttributes.createDelimitedFormat(
        "\t",
        "\\",
        "\""
      )
    )
  }

}

