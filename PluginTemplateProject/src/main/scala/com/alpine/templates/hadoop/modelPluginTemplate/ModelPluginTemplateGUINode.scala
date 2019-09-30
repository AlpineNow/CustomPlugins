package com.alpine.templates.hadoop.modelPluginTemplate

import com.alpine.metadata.RegressionModelMetadata
import com.alpine.plugin.OperatorDesignContext
import com.alpine.plugin.core.{OperatorGUINode, OperatorStatus}
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, HdfsTabularDataset, OperatorSchemaManager}
import com.alpine.plugin.core.utils.SparkParameterUtils
import com.alpine.plugin.model.RegressionModelWrapper

// TODO: Define the operator UI and associated behaviour
class ModelPluginTemplateGUINode extends OperatorGUINode[HdfsTabularDataset, RegressionModelWrapper] {
  import ModelPluginTemplateUtils._

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    // Dependent variable selection
    operatorDialog.addTabularDatasetColumnDropdownBox(
      id = stringDependentColumnID,
      label = stringDependentColumn,
      columnFilter = ColumnFilter.NumericOnly,
      selectionGroupId = "main",
      required = true);

    // Predictor variables selection
    operatorDialog.addTabularDatasetColumnCheckboxes(
      id = stringIndependentColumnsID,
      label = stringIndependentColumns,
      columnFilter = ColumnFilter.All,
      selectionGroupId = "main",
      required = true);

    SparkParameterUtils.addStandardSparkOptions(
      operatorDialog,
      additionalSparkParameters = List(SparkParameterUtils.makeStorageLevelParam("NONE")));
  }

  // TODO: Do input validation: Validate combination of parameters that cannot be done on UI
  override def getOperatorStatus(context: OperatorDesignContext): OperatorStatus = {
    if (context.inputSchemas.isEmpty)
      OperatorStatus(isValid = false)
    else {
      val parameters = context.parameters
      val inputColsMap: Map[String, ColumnDef] = context.inputSchemas.head._2.definedColumns.map(c => (c.columnName, c)).toMap
      val inputFeatures: Seq[ColumnDef] = parameters.getTabularDatasetSelectedColumnNames(stringIndependentColumnsID)
        .flatMap(name => inputColsMap.get(name));

      val stringErrorMsg = "";
      if (stringErrorMsg.nonEmpty)
        OperatorStatus(isValid = false, stringErrorMsg);
      else {
        // This is metadata for the predictor node, sets the schema of predictor columns to be  expected for scoring
        // and string to be appended to the prediction column name to be produced
        val outputMetadata = new RegressionModelMetadata(
          Some(inputFeatures), // predictor column definitions
          "mymlmodel", // model name, this will be appended to the predicted value column name in predictor
          sqlScorable = false, // set this to true if you implement sqltransformer
          inputColsMap.get(parameters.getTabularDatasetSelectedColumnName(stringDependentColumnID)));

        OperatorStatus(isValid = true, None, outputMetadata)

        OperatorStatus(isValid = true);
      }
    }
  }
}
