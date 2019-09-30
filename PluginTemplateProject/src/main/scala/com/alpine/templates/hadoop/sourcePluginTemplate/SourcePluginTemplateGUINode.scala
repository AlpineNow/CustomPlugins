package com.alpine.templates.hadoop.sourcePluginTemplate

import com.alpine.plugin.core.{OperatorGUINode, OperatorParameters, OperatorStatus}
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io.{HdfsTabularDataset, IONone, OperatorSchemaManager, TabularSchema}
import com.alpine.plugin.core.utils.{HdfsParameterUtils, HdfsStorageFormatType, SparkParameterUtils}

class SourcePluginTemplateGUINode extends OperatorGUINode[IONone, HdfsTabularDataset]{
  import SourcePluginTemplateUtils._

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    // add a parameter to the dialog box which lets the user enumerate the number of things (the
    // length of the dataset) she wants to generate.
    operatorDialog.addIntegerBox(
      id = numberRowsParamKey,
      label = numberRowsParamName,
      min = 0,
      max = 100,
      defaultValue = 10);

    /*
    Use the HdfsParameterUtils class to add a dropdown box which will let the user determine
    how the output data is stored (CSV, Avro, Parquet). Default to CSV.
     */
    HdfsParameterUtils.addHdfsStorageFormatParameter(operatorDialog, HdfsStorageFormatType.CSV)
    /*
     * Use the HdfsParameterUtils class to add parameters about how/where to store the output
     * dataset. In particular: the output directory, the output name, and whether to overwrite
     * anything already stored at that location.
     */
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)
    /*
     * Use the SparkParameterUtils class to add the some parameters to let the user configure the
     * Spark job.
     */
    SparkParameterUtils.addStandardSparkOptions(
      operatorDialog,
      additionalSparkParameters = List());
  }

  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    // update the schema
    // this is using the value chosen by the user in the Alpine GUI
    operatorSchemaManager.setOutputSchema(getOutputSchema(params))
    // return the operator status with isValid = true.
    OperatorStatus(isValid = true)
  }

}

