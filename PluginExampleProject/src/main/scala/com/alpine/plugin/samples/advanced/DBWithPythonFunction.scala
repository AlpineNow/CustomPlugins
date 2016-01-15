package com.alpine.plugin.samples.advanced

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.db.{DBExecutionContext, DBRuntime}
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.DBTableDefault
import com.alpine.plugin.core.utils.DBParameterUtils

/**
  * WARNING : Will only work on GreenPlum databases.
  *
  * Given a numeric column of seeds, creates a one column table with a random number generated based
  * generated based on the seed.
  * Uses pyhon to generate the random number.
  *
  */
class DBWithPythonOperatorSignature extends OperatorSignature[
  DBWithPythonGUINode,
  DBWithPythonRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - DB With Python",
      category = "Plugin Sample - DB",
      author = "Jenny Thompson",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

object DBWithPythonContansts{
  //the name of the new column that we are returning as output
  val outputColumnName = "random_column"
  val outputSchema =  TabularSchema(
    Seq(
      ColumnDef(outputColumnName, ColumnType.TypeValue("DOUBLE PRECISION")
      )
    )
  )

  val functionNameParamId = "function_name"
  val randomSeedColParamId = "seedColumn"
}

class DBWithPythonGUINode extends OperatorGUINode[
  DBTable,
  DBTable] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnDropdownBox(
      DBWithPythonContansts.randomSeedColParamId ,
      "Random Seed",
      ColumnFilter.All,
      "main"
    )

    operatorDialog.addStringBox(
      id = DBWithPythonContansts.functionNameParamId,
      label = "Name of Random Function",
      defaultValue = "random_func",
      //this is a regex that insures that the function name starts with a letter and doesn't contain illegal characters
      regex = "^[A-Za-z]+\\w*$",
      width = 0,
      height = 0
    )

    //add parameters to let the user determine how the output table will be written
    DBParameterUtils.addStandardDatabaseOutputParameters(operatorDialog, operatorDataSourceManager)

  }

  /**
    *Update the schema to include one extra column, with a random number
    */
  private def updateOutputSchema(inputSchemas: Map[String, TabularSchema],
                                 params: OperatorParameters,
                                 operatorSchemaManager: OperatorSchemaManager): Unit = {
     operatorSchemaManager.setOutputSchema(DBWithPythonContansts.outputSchema)
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

class DBWithPythonRuntime extends DBRuntime[DBTable, DBTable] {

  override def onExecution(context: DBExecutionContext,
                           input: DBTable,
                           params: OperatorParameters,
                           listener: OperatorListener): DBTable = {

    //get the output parameters
    val outputSchema = DBParameterUtils.getDBOutputSchemaParam(params)
    val isView = DBParameterUtils.getIsViewParam(params)
    val outputName = DBParameterUtils.getResultTableName(params)
    val connectionInfo = context.getDBConnectionInfo

    //check if there is a table  or with the same name as the output table and drop according to the
    // "overwrite"
    val overwrite = DBParameterUtils.getOverwriteParameterValue(params)
    val fullOutputName = getQuotedSchemaTableName(outputSchema, outputName)

    val stmt = connectionInfo.connection.createStatement()

    if (overwrite) {
      //First see if a table of that name exists.
      // This will throw an exception if there is a view with the output name,
      // we will catch the exception and delete the view in the next block of code.

      try {
        listener.notifyMessage("Dropping table if it exists")
        val dropTableStatementBuilder = new StringBuilder()
        dropTableStatementBuilder ++= "DROP TABLE IF EXISTS " + fullOutputName + " CASCADE;"
        stmt.execute(dropTableStatementBuilder.toString())
      }
      catch {
        case (e: Exception) => listener.notifyMessage("A view of the name " + fullOutputName + "exists");
      }

      //Now see if there is a view with the output name
      listener.notifyMessage("Dropping view if it exists")
      val dropViewStatementBuilder = new StringBuilder()
      dropViewStatementBuilder ++= "DROP VIEW IF EXISTS " + fullOutputName + " CASCADE;"
      stmt.execute(dropViewStatementBuilder.toString())
    }

    val functionName = params.getStringValue(DBWithPythonContansts.functionNameParamId)
    //create a sql query with the Python code to generate the random number inside it
    val createFunctionSQL =
    //if overwrite then override the name space of the new function otherwise will fail is a function
    //on the database is alredy registered under that name
      s"""CREATE ${if (overwrite) "OR REPLACE" else ""} FUNCTION $functionName(seed numeric) """ +
      """RETURNS numeric AS $$
         |    if 'random' not in GD:
         |        import random
         |        GD['random'] = random
         |    GD['random'].seed(seed)
         |    return GD['random'].random()
         |$$ LANGUAGE plpythonu;""".stripMargin

    val seedColumnName =
      params.getTabularDatasetSelectedColumn(DBWithPythonContansts.randomSeedColParamId)._2

    val sqlStatementBuilder = new StringBuilder()
    if (isView) {
      sqlStatementBuilder ++= "CREATE VIEW " + fullOutputName + " AS ("
    } else {
      sqlStatementBuilder ++= "CREATE TABLE " + fullOutputName + " AS ("
    }

    sqlStatementBuilder ++= "SELECT "
    sqlStatementBuilder ++= functionName + "(" + quoteName(seedColumnName) + ") AS " +
      DBWithPythonContansts.outputColumnName
    sqlStatementBuilder ++= " FROM " + getQuotedSchemaTableName(input.schemaName, input.tableName) + ");"

    try {
      //execute the sql statement
      stmt.execute(createFunctionSQL)
      stmt.execute(sqlStatementBuilder.toString())
    } finally {
      //make sure statement is closed
      stmt.close()
    }

    //create the output schema
    val outputTabularSchema = DBWithPythonContansts.outputSchema

    //return the alpine IOBase type with the meta data about the new database output
    DBTableDefault(
      outputSchema,
      outputName,
      outputTabularSchema,
      isView,
      connectionInfo.name,
      connectionInfo.url,
      Some(params.operatorInfo)
    )
  }

  /**
    * These function are needed to handle column names with spaces or irregular characters
    */
  def getQuotedSchemaTableName(schemaName: String, tableName: String): String = {
    quoteName(schemaName) + "." + quoteName(tableName)
  }

  def quoteName(colName: String): String = {
    "\"" + colName + "\""
  }

}