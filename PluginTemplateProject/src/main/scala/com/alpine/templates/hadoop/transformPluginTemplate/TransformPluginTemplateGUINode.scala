package com.alpine.templates.hadoop.transformPluginTemplate

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{ColumnDef, OperatorSchemaManager, TabularSchema}
import com.alpine.plugin.core.spark.templates.SparkDataFrameGUINode

class TransformPluginTemplateGUINode extends SparkDataFrameGUINode[TransformPluginTemplateJob]{
  import TransformPluginTemplateUtils._

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnCheckboxes(
      id = parameterID,      // the ID of the operator
      label = parameterName,
      columnFilter = ColumnFilter.All,
      selectionGroupId = "main");
  }

  // TODO: Function that specifies the schema of the operator's output dataframe, so the downstream operators can infe
  //  column metadata. Change to enforce your schema
  override def defineOutputSchemaColumns(inputSchema: TabularSchema,
                                         parameters: OperatorParameters): Seq[ColumnDef] = {

    val columnsToKeep = parameters.getTabularDatasetSelectedColumns(parameterID)._2
    inputSchema.getDefinedColumns.filter(colDef => columnsToKeep.contains(colDef.columnName))
  }

}

