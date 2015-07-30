package com.alpine.plugin.samples.ver1_0

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.utils.OutputParameterUtils
import com.alpine.plugin.model.RegressionModelWrapper

/**
 * @author Jenny Thompson
 *         7/21/15
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

