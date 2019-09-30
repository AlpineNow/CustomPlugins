package com.alpine.templates.hadoop.visualPluginTemplate

import com.alpine.plugin.core.OperatorGUINode
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.{HdfsTabularDataset, IONone, OperatorSchemaManager}

/**
 * The JavascriptVisualizationGUINode defines the design time behavior of the plugin. It extends
 * the template GUI class "SparkDataFrameGUINode".
 * It is in this class that we define the parameters which the user will see when they click on the
 * operator and the design time output schema.
 */
// TODO: Define the operator UI and associated behaviour
class VisualPluginTemplateGUINode extends OperatorGUINode[HdfsTabularDataset, IONone] {
  import VisualPluginTemplateUtils._

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnDropdownBox(
      id = SRC_ID,
      label = SRC_LABEL,
      columnFilter = ColumnFilter.All,
      selectionGroupId = "main",
      required = true);

    operatorDialog.addTabularDatasetColumnDropdownBox(
      id = DST_ID,
      label = DST_LABEL,
      ColumnFilter.All,
      selectionGroupId = "main",
      required = true);
  }

}


