package com.alpine.templates.db
//todo - copy this template into your your own package and change the package name
//todo - update pom.xml to have your own jar for uploading into Alpine

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.db.{DBExecutionContext, DBRuntime}
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.DBTableDefault
import com.alpine.plugin.core.utils.DBParameterUtils

/**
 * The Signature class of Plugin. This defines how the plugin will appear both in the list of
 * jars and plugins in the "Manage Custom Operators" tab and in the Alpine GUI.
 * The class takes two type parameters:
 * - DBTransformationTemplateGUINode: extends OperatorGUINode and
 * defines the design time behavior of the plugin (including the parameter definitions, how the
 * output will be visualized, and the design time output schema).
 * - DBTransformationTemplateRuntime: extends DBRuntime and defines the runtime behavior of the operator.
 */

//TODO - rename Signature, GUINode and Runtime classes to refer to your operator
//TODO - update plugins.xml to include your new Signature name
class DBTransformationTemplateSignature extends OperatorSignature[
  DBTransformationTemplateGUINode,
  DBTransformationTemplateRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Operator Name",   //TODO - name your operator with your name
      category = "",            //TODO - put a category here
      author = "",              //TODO - put your name here
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

/**
 * Util with the constants for the plugin that will be used in multiple classes.
 */
object DBTransformationTemplateConstants {
  //TODO - keep parameter value keys in here for access across multiple classes
}

/**
 * The DBTransformationTemplateGUINode defines the design time behavior of the plugin. It is in this
 * class that we define the parameters which the user will see when they click on the
 * operator as well as the design time output schema.
 */
class DBTransformationTemplateGUINode extends OperatorGUINode[
  DBTable,
  DBTable] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    //TODO - add needed parameters

    //utility class to add standard parameters that let the user determine how the output table will be written
    DBParameterUtils.addStandardDBOutputParameters(operatorDialog)
  }


  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    this.updateOutputSchema(
      inputSchemas,
      params,
      operatorSchemaManager
    )
    //TODO - add any validation code, if necessary
    OperatorStatus(isValid = true, msg = None)
  }


  private def updateOutputSchema(inputSchemas: Map[String, TabularSchema],
                                 params: OperatorParameters,
                                 operatorSchemaManager: OperatorSchemaManager): Unit = {

    //TODO - create an output TabularSchema "outputSchema"
    //TODO - set the output schema of the operatorSchemaManager to "outputSchema"
    //NOTE - for now, output schema = input schema
    if (inputSchemas.nonEmpty) {
      val inputSchema = inputSchemas.values.head
      operatorSchemaManager.setOutputSchema(inputSchema)
    }
  }

}

/**
 * The DBTransformationTemplateRuntime defines the runtime behavior of the operator.
 * WARNING:  All execution code is written for GPDB / PostreSQL.  This will not necessarily
 * work on other databases.
 */
class DBTransformationTemplateRuntime extends DBRuntime[DBTable, DBTable] {

  override def onExecution(context: DBExecutionContext,
                           input: DBTable,
                           params: OperatorParameters,
                           listener: OperatorListener): DBTable = {

    //get the basic database output parameters
    val outputSchema = DBParameterUtils.getDBOutputSchemaParam(params)
    val isView = DBParameterUtils.getIsViewParam(params)
    val outputName = DBParameterUtils.getResultTableName(params)
    val connectionInfo = context.getDBConnectionInfo
    val overwrite = DBParameterUtils.getOverwriteParameterValue(params)
    val generator = context.getSQLGenerator
    val fullOutputName = generator.quoteObjectName(outputSchema, outputName)

    val sqlExecutor = context.getSQLExecutor
    if (overwrite) {
      sqlExecutor.ddlDropTableOrViewIfExists(fullOutputName)
    }
    val sourceTableFullName = generator.quoteObjectName(input.schemaName, input.tableName)

    //TODO - pull out values from custom parameters

    //TODO - create the sql statement that will be executed using the parameters
    val sqlStatement = generator.getCreateTableOrViewAsSelectSQL(
      columns = "*",sourceTable = sourceTableFullName, destinationTable = fullOutputName, isView = isView
    )

    val stmt = connectionInfo.connection.createStatement()
    try {
      stmt.execute(sqlStatement)
    } finally {
      stmt.close()
    }

    //TODO - create the output schema (set to input schema for now)
    val outputTabularSchema = input.tabularSchema
    //
    //metadata about output table.  At runtime, next operator uses this to know where the data is.  Also passed to visualization function. However, default works so not overriding it.
    /**
     *  returns implementation of DBTable.  Used in two ways:
     *  (1) At runtime, the following operator uses this information to know where its input data is coming from
     *  (2) At runtime, this object is passed to the visualization function (not applicable here as we use the default visualization for a DBTable)
     */
    DBTableDefault(
      outputSchema,
      outputName,
      outputTabularSchema
    )
  }

}
