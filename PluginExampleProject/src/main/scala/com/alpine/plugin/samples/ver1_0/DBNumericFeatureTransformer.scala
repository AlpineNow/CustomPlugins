/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.samples.ver1_0

import scala.collection.mutable

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.utils.SchemaUtils
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}

class DBNumericFeatureTransformerSignature extends OperatorSignature[
  DBNumericFeatureTransformerGUINode,
  DBNumericFeatureTransformerRuntime]{
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
  override def onPlacement(
    operatorDialog: OperatorDialog,
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
      Array("Pow2", "Pow3").toSeq,
      "Pow2"
    )

    operatorDialog.addDBSchemaDropdownBox(
      "outputSchema",
      "Output Schema",
      "",
      operatorDataSourceManager
    )

    operatorDialog.addRadioButtons(
      "viewOrTable",
      "Output Type",
      Array("View", "Table").toSeq,
      "View"
    )

    operatorDialog.addRadioButtons(
      "overwrite",
      "Overwrite Output",
      Array("Yes", "No").toSeq,
      "Yes"
    )

    operatorDialog.addStringBox(
      "outputName",
      "Output Name",
      "tmp",
      ".+",
      0,
      0
    )
  }

  private def updateOutputSchema(
    inputSchemas: mutable.Map[String, TabularSchemaOutline],
    params: OperatorParameters,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    // There can only be one input schema.
    if (inputSchemas.size > 0) {
      val inputSchema = inputSchemas.values.iterator.next()
      if (inputSchema.getFixedColumns().length > 0) {
        val numInputColumns = inputSchema.getMaxNumColumns()
        val (_, columnsToTransform) =
          params.getTabularDatasetSelectedColumns("columnsToTransform")
        val transformationType = params.getStringValue("transformationType")
        val numTransformedColumns = columnsToTransform.length
        val outputSchema = operatorSchemaManager.createTabularSchemaOutline(
          minNumCols = numInputColumns + numTransformedColumns,
          maxNumCols = numInputColumns + numTransformedColumns
        )

        SchemaUtils.copyColumnDefs(inputSchema, outputSchema)
        var i = 0
        while (i < columnsToTransform.length) {
          outputSchema.addColumnDef(
            ColumnDef(
              columnsToTransform(i) + "_" + transformationType.toLowerCase,
              ColumnType.TypeValue("DOUBLE PRECISION")
            )
          )
          i += 1
        }

        operatorSchemaManager.setOutputSchemaOutline(outputSchema)
      }
    }
  }

  override def onInputOrParameterChange(
    inputSchemas: mutable.Map[String, TabularSchemaOutline],
    params: OperatorParameters,
    operatorSchemaManager: OperatorSchemaManager): Unit = {
    this.updateOutputSchema(
      inputSchemas,
      params,
      operatorSchemaManager
    )
  }

  /*
  override def onOutputVisualization(
    params: OperatorParameters,
    output: DBTable,
    visualFactory: VisualModelFactory): VisualModel = {
    visualFactory.createTextVisualization(
      output.getDictValue("TestValue1").toString + " " +
        output.getDictValue("TestValue2").toString
    )
  }
  */
}

class DBNumericFeatureTransformerRuntime extends DatabaseRuntime[DBTable, DBTable] {
  override def onExecution(
    context: DBExecutionContext,
    input: DBTable,
    params: OperatorParameters,
    listener: OperatorListener,
    ioFactory: IOFactory): DBTable = {
    val (_, columnsToTransform) =
      params.getTabularDatasetSelectedColumns("columnsToTransform")
    val transformationType = params.getStringValue("transformationType")
    val outputSchema = params.getStringValue("outputSchema")
    val isView = params.getStringValue("viewOrTable").equals("View")
    val overwrite = params.getStringValue("overwrite").equals("Yes")
    val outputName = params.getStringValue("outputName").trim
    val connectionInfo = context.getDBConnectionInfo

    if (overwrite) {
      val dropSqlStatementBuilder = new StringBuilder()
      if (isView) {
        dropSqlStatementBuilder ++= "DROP VIEW IF EXISTS " + outputSchema + "." + outputName + ";"
      } else {
        dropSqlStatementBuilder ++= "DROP TABLE IF EXISTS " + outputSchema + "." + outputName + ";"
      }

      val stmt = connectionInfo.connection.createStatement()
      stmt.execute(dropSqlStatementBuilder.toString())
      stmt.close()
    }

    val sqlStatementBuilder = new StringBuilder()
    if (isView) {
      sqlStatementBuilder ++= "CREATE VIEW " + outputSchema + "." + outputName + " AS ("
    } else {
      sqlStatementBuilder ++= "CREATE TABLE " + outputSchema + "." + outputName + " AS ("
    }

    val inputSchemaOutline = input.getSchemaOutline()
    val columnDefs = inputSchemaOutline.getFixedColumns()
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
        sqlStatementBuilder ++= " FROM " + input.getTableName() + ");"
      }
    }

    val stmt = connectionInfo.connection.createStatement()
    stmt.execute(sqlStatementBuilder.toString())
    stmt.close()

    val numInputColumns = columnDefs.length
    val numTransformedColumns = columnsToTransform.length

    // Don't close the connection.
    val outputSchemaOutline = ioFactory.createTabularSchemaOutline(
      minNumCols = numInputColumns + numTransformedColumns,
      maxNumCols = numInputColumns + numTransformedColumns
    )

    SchemaUtils.copyColumnDefs(input.getSchemaOutline(), outputSchemaOutline)
    i = 0
    while (i < columnsToTransform.length) {
      outputSchemaOutline.addColumnDef(
        ColumnDef(
          columnsToTransform(i) + "_" + transformationType,
          ColumnType.TypeValue("DOUBLE PRECISION")
        )
      )
      i += 1
    }

    val output =
      ioFactory.createDBTable(
        outputSchema,
        outputName,
        outputSchemaOutline,
        isView,
        connectionInfo
      )

    output.setDictValue("TestValue1", new Integer(1))
    output.setDictValue("TestValue2", new Integer(2))

    output
  }

  override def onStop(
    context: DBExecutionContext,
    listener: OperatorListener): Unit = {
    // Do nothing.
  }
}