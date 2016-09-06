/*
 * Copyright (c) 2015 Alpine Data Labs
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

package com.alpine.plugin.samples.ver1_0

import java.sql.SQLException

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.db.{DBExecutionContext, DBRuntime}
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.DBTableDefault
import com.alpine.plugin.core.utils.DBParameterUtils

/**
  * Ths operator has the same behavior as the Numeric Feature Transformer but works with Database input
  * rather than hadoop.
  */
class DBNumericFeatureTransformerSignature extends OperatorSignature[
  DBNumericFeatureTransformerGUINode,
  DBNumericFeatureTransformerRuntime] {

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name = "Sample - DB Transform",
    category = "Plugin Sample - DB",
    author = Some("Sung Chung"),
    version = 1,
    helpURL = None,
    icon = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. This will appear when a user hovers " +
      "over the operator’s name in the workflow editor. The best tooltips concisely describe the function" +
      " of the operator and are no more than fifty words.")
  )
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
      ColumnFilter.NumericOnly,
      "main"
    )

    operatorDialog.addDropdownBox(
      "transformationType",
      "Transformation type",
      Seq("Pow2", "Pow3"),
      "Pow2"
    )

    //add parameters to let the user determine how the output table will be written
    DBParameterUtils.addStandardDBOutputParameters(operatorDialog)

  }

  private def updateOutputSchema(inputSchemas: Map[String, TabularSchema],
                                 params: OperatorParameters,
                                 operatorSchemaManager: OperatorSchemaManager): Unit = {
    // There can only be one input schema.
    if (inputSchemas.nonEmpty) {
      val inputSchema = inputSchemas.values.iterator.next()
      if (inputSchema.getDefinedColumns.nonEmpty) {
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

  override def onInputOrParameterChange(inputSchemas: Map[String, TabularSchema],
                                        params: OperatorParameters,
                                        operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    this.updateOutputSchema(
      inputSchemas,
      params,
      operatorSchemaManager
    )

    OperatorStatus(isValid = true, msg = None)
  }

}

object SchemaTransformer {
  /**
    * Transform the database schema to the alpine output schema.
    */
  def transform(inputSchema: TabularSchema,
                columnsToTransform: Array[String],
                transformationType: String): TabularSchema = {
    val outputColumnDefs = inputSchema.getDefinedColumns ++ columnsToTransform.map(
      columnName => {
        ColumnDef(
          getOutputColumnName(columnName, transformationType),
          ColumnType.TypeValue("DOUBLE PRECISION")
        )
      }
    )

    TabularSchema(outputColumnDefs)
  }

  def getOutputColumnName(columnToTransform: String, transformationType: String): String = {
    columnToTransform + "_" + transformationType
  }
}

class DBNumericFeatureTransformerRuntime extends DBRuntime[DBTable, DBTable] {

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
    val statement = connectionInfo.connection.createStatement()

    try {
      val sqlGenerator = context.getSQLGenerator
      //check if there is a table  or with the same name as the output table and drop according to the
      // "overwrite"
      val overwrite = DBParameterUtils.getDropIfExistsParameterValue(params)
      val fullOutputName = sqlGenerator.quoteIdentifier(outputSchema) + "." + sqlGenerator.quoteIdentifier(outputName)
      if (overwrite) {
        //First see if a table of that name exists.
        // This will throw an exception if there is a view with the output name,
        // we will catch the exception and delete the view in the next block of code.
        try {
          listener.notifyMessage("Dropping table if it exists")
          statement.execute(sqlGenerator.getDropTableIfExistsSQL(fullOutputName, cascade = true))
        }
        catch {
          case (e: SQLException) =>
        }
        //Now see if there is a view with the output name
        listener.notifyMessage("Dropping view if it exists")
        try {
          statement.execute(sqlGenerator.getDropViewIfExistsSQL(fullOutputName, cascade = true))
        } catch {
          case (e: SQLException) =>
        }
      }

      val inputSchema = input.tabularSchema
      val columnDefs = inputSchema.getDefinedColumns
      val passThroughColumnsSQL = columnDefs.map(
        columnDef => sqlGenerator.quoteIdentifier(columnDef.columnName)
      ).mkString(", ")

      val power =
        if (transformationType.equals("Pow2")) {
          "2"
        } else {
          "3"
        }
      val transformedColumnsSQL = columnsToTransform.map(
        columnToTransform =>
          "POWER(" + sqlGenerator.quoteIdentifier(columnToTransform) + ", " + power + ") AS " +
            sqlGenerator.quoteIdentifier(SchemaTransformer.getOutputColumnName(columnToTransform, transformationType))
      ).mkString(", ")

      val createOutputSQL = sqlGenerator.getCreateTableOrViewAsSelectSQL(
        columns = passThroughColumnsSQL + ", " + transformedColumnsSQL,
        sourceTable = sqlGenerator.quoteIdentifier(input.schemaName) + "." + sqlGenerator.quoteIdentifier(input.tableName),
        destinationTable = fullOutputName,
        isView = isView
      )

      statement.execute(createOutputSQL)
    } finally {
      statement.close()
    }

    //create the output schema
    val outputTabularSchema =
      SchemaTransformer.transform(
        input.tabularSchema,
        columnsToTransform,
        transformationType
      )

    DBTableDefault(
      outputSchema,
      outputName,
      outputTabularSchema,
      isView,
      connectionInfo.name,
      connectionInfo.url,
      //save keys to the output to create visualizations
      Map("TestValue1" -> new Integer(1), "TestValue2" -> new Integer(2))
    )
  }

}