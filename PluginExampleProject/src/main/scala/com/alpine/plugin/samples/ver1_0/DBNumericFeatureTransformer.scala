/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0


import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.DBTableDefault
import com.alpine.plugin.core.utils.DBParameterUtils

import scala.collection.mutable


class DBNumericFeatureTransformerSignature extends OperatorSignature[
  DBNumericFeatureTransformerGUINode,
  DBNumericFeatureTransformerRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "DBNumericFeatureTransformer",
      category = "Transformation",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class DBNumericFeatureTransformerGUINode extends OperatorGUINode[
  DBTable,
  DBTable] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnCheckboxes(
      "columnsToTransform",
      "Columns to transform",
      // Need to figure out what numeric types the intended DBs have.
      // Before that, we'll simply allow all column types to be shown.
      ColumnFilter.All,
      "main"
    )

    operatorDialog.addDropdownBox(
      "transformationType",
      "Transformation type",
      Array("Pow2", "Pow3").toSeq,
      "Pow2"
    )

    //add parameters to let the user determine how the output table will be written
    DBParameterUtils.addStandardDatabaseOutputParameters(operatorDialog, operatorDataSourceManager)

  }

  private def updateOutputSchema(inputSchemas: mutable.Map[String, TabularSchema],
                                 params: OperatorParameters,
                                 operatorSchemaManager: OperatorSchemaManager): Unit = {
    // There can only be one input schema.
    if (inputSchemas.nonEmpty) {
      val inputSchema = inputSchemas.values.iterator.next()
      if (inputSchema.getDefinedColumns().nonEmpty) {
        val (_, columnsToTransform) =
          params.getTabularDatasetSelectedColumns("columnsToTransform")
        val transformationType = params.getStringValue("transformationType")
        val outputSchema = SchemaTransformer.transform(
          inputSchema,
          columnsToTransform,
          transformationType
        )

        operatorSchemaManager.setOutputSchema(
          outputSchema
        )
      }
    }
  }

  override def onInputOrParameterChange(inputSchemas: mutable.Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager): Unit = {
    this.updateOutputSchema(
      inputSchemas,
      params,
      operatorSchemaManager
    )
  }

}

object SchemaTransformer {
  /**
   * Transform the database schema to the alpine output schema.
   */
  def transform(inputSchema: TabularSchema,
                columnsToTransform: Array[String],
                transformationType: String): TabularSchema = {
    val outputColumnDefs = mutable.ArrayBuffer[ColumnDef]()
    outputColumnDefs ++= inputSchema.getDefinedColumns()
    var i = 0
    while (i < columnsToTransform.length) {
      outputColumnDefs +=
        ColumnDef(
          columnsToTransform(i) + "_" + transformationType.toLowerCase,
          ColumnType.TypeValue("DOUBLE PRECISION")
        )
      i += 1
    }

    TabularSchema(outputColumnDefs)
  }
}

class DBNumericFeatureTransformerRuntime extends DatabaseRuntime[DBTable, DBTable] {

  override def onExecution(context: DBExecutionContext,
                           input: DBTable,
                           params: OperatorParameters,
                           listener: OperatorListener): DBTable = {
    val (_, columnsToTransform) =
      params.getTabularDatasetSelectedColumns("columnsToTransform")
    val transformationType = params.getStringValue("transformationType")

    //get the output parameters
    val outputSchema = DBParameterUtils.getDBOutputSchemaParam(params)
    val isView = DBParameterUtils.getIsViewParam(params)
    val outputName = DBParameterUtils.getResultTableName(params)
    val connectionInfo = context.getDBConnectionInfo

    //check if there is a table  or with the same name as the output table and drop according to the
    // "overwrite"
    val overwrite = DBParameterUtils.getOverwriteParameterValue(params)
    val fullOutputName = getQuoteOutputName(outputName, outputSchema)
    if (overwrite) {
      val stmtTable = connectionInfo.connection.createStatement()
      //First see if a table of that name exists.
      // This will throw an exception if there is a view with the output name,
      // we will catch the exception and delete the view in the next block of code.
      try {
        listener.notifyMessage("Dropping table if it exists")
        val dropTableStatementBuilder = new StringBuilder()
        dropTableStatementBuilder ++= "DROP TABLE IF EXISTS " + fullOutputName + " CASCADE;"
        stmtTable.execute(dropTableStatementBuilder.toString())
      }
      catch {
        case (e: Exception) => listener.notifyMessage("A view of the name " + fullOutputName + "exists");
      }
      finally {
        stmtTable.close()
      }
     //Now see if there is a view with the output name
      listener.notifyMessage("Dropping view if it exists")
      val dropViewStatementBuilder = new StringBuilder()
      dropViewStatementBuilder ++= "DROP VIEW IF EXISTS " + fullOutputName + " CASCADE;"
      val stmtView = connectionInfo.connection.createStatement()
      stmtView.execute(dropViewStatementBuilder.toString())
      stmtView.close()
    }

    val sqlStatementBuilder = new StringBuilder()
    if (isView) {
      sqlStatementBuilder ++= "CREATE VIEW " + fullOutputName + " AS ("
    } else {
      sqlStatementBuilder ++= "CREATE TABLE " + fullOutputName + " AS ("
    }

    val inputSchema = input.getTabularSchema()
    val columnDefs = inputSchema.getDefinedColumns()
    sqlStatementBuilder ++= "SELECT "
    var i = 0
    while (i < columnDefs.length) {
      val columnDef = columnDefs(i)
      sqlStatementBuilder ++= columnDef.columnName + ", "
      i += 1
    }

    i = 0
    while (i < columnsToTransform.length) {
      val columnToTransform = columnsToTransform(i)
      val power =
        if (transformationType.equals("Pow2")) {
          "2"
        } else {
          "3"
        }
      sqlStatementBuilder ++= "POWER(" + columnToTransform + ", " + power + ") AS "
      sqlStatementBuilder ++= columnToTransform + "_pow" + power
      i += 1

      if (i != columnsToTransform.length) {
        sqlStatementBuilder ++= ", "
      } else {
        sqlStatementBuilder ++= " FROM " + input.getSchemaName() + "." + input.getTableName() + ");"
      }
    }
    val stmt = connectionInfo.connection.createStatement()
    stmt.execute(sqlStatementBuilder.toString())
    stmt.close()

    //create the output schema
    val outputTabularSchema =
      SchemaTransformer.transform(
        input.getTabularSchema(),
        columnsToTransform,
        transformationType
      )

    val output = new DBTableDefault(
      outputSchema,
      outputName,
      outputTabularSchema,
      isView,
      connectionInfo
    )
    //save keys to the output to create visualizations
    output.setDictValue("TestValue1", new Integer(1))
    output.setDictValue("TestValue2", new Integer(2))

    output
  }

  def getQuoteOutputName(tableName: String, schemaName: String): String = {
    "\"" + schemaName + "\"" + "." + "\"" + tableName + "\""
  }

}