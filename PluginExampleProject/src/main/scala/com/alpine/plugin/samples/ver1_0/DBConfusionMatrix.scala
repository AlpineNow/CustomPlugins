package com.alpine.plugin.samples.ver1_0

import com.alpine.model.ClassificationRowModel
import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.multiple.CombinerModel
import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.db.{DBExecutionContext, DBRuntime}
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.DBTableDefault
import com.alpine.plugin.core.utils.DBParameterUtils
import com.alpine.plugin.model.ClassificationModelWrapper
import com.alpine.sql.DatabaseType.TypeValue
import com.alpine.sql.{AliasGenerator, DatabaseType, SQLGenerator}
import com.alpine.transformer.sql.ColumnName
import com.alpine.util.SQLUtility

/**
  * This takes in a Classification model (e.g. Logistic Regression) and a database table.
  * It calculates the confusion matrix of the model using the table as the test data.
  *
  * WARNING: Due to the DROP TABLE SQL, it will only work on PostgreSQL / Greenplum / HAWQ.
  *
  * This is a non-terminal operator (it can be connected to subsequent operators).
  * The output table is the matrix in sparse format.
  * e.g.
  * --------------------------------
  * Observed | Predicted | N (count)
  * --------------------------------
  * no       | no        | 4
  * no       | yes       | 1
  * yes      | no        | 2
  * yes      | yes       | 7
  * --------------------------------
  */
class DBConfusionMatrixSignature extends OperatorSignature[
  DBConfusionMatrixGUINode,
  DBConfusionMatrixRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - DB Confusion Matrix",
      category = "Plugin Sample - DB",
      author = "Jenny Thompson",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class DBConfusionMatrixGUINode extends OperatorGUINode[
  Tuple2[ClassificationModelWrapper, DBTable],
  DBTable] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    //add parameters to let the user determine how the output table will be written
    DBParameterUtils.addStandardDatabaseOutputParameters(operatorDialog, operatorDataSourceManager)
    operatorSchemaManager.setOutputSchema(ConfusionMatrixUtils.outputTabularSchema)
  }

}

object ConfusionMatrixUtils {

  val observedColumnName: String = "Observed"
  val predictedColumnName: String = "Predicted"
  val countColumnName: String = "N" // Don't use "count" as it is a SQL keyword.

  val outputTabularSchema = TabularSchema(
    Seq(
      ColumnDef(observedColumnName, ColumnType.String),
      ColumnDef(predictedColumnName, ColumnType.String),
      ColumnDef(countColumnName, ColumnType.String)
    )
  )

}

class DBConfusionMatrixRuntime extends DBRuntime[Tuple2[ClassificationModelWrapper, DBTable], DBTable] {

  override def onExecution(context: DBExecutionContext,
                           input: Tuple2[ClassificationModelWrapper, DBTable],
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
        case (e: Exception) => listener.notifyMessage("A view of the name " + fullOutputName + " exists");
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

    val createTableSQL: String = getCreateTableSQL(input, isView, fullOutputName)

    val stmt = connectionInfo.connection.createStatement()
    stmt.execute(createTableSQL)
    stmt.close()

    DBTableDefault(
      outputSchema,
      outputName,
      ConfusionMatrixUtils.outputTabularSchema,
      isView,
      connectionInfo.name,
      connectionInfo.url,
      Some(params.operatorInfo)
    )
  }

  def getCreateTableSQL(input: Tuple2[_ <: ClassificationModelWrapper, _ <: DBTable],
                        isView: Boolean,
                        fullOutputName: String): String = {
    val inputDataset: DBTable = input._2
    val inputModel: ClassificationRowModel = input._1.model

    /**
      * TODO: Will expose official SQLGenerators to the onExecution method in the future.
      */
    val sqlGenerator: SQLGenerator = new SQLGenerator {
      override def useAliasForSelectSubQueries: Boolean = true

      override def quoteChar: Char = '"'

      override def escapeColumnName(s: String): String = "\"" + s + "\""

      override def dbType: TypeValue = DatabaseType.greenplum
    }

    val sqlTransformerOption = inputModel.sqlTransformer(sqlGenerator)

    if (sqlTransformerOption.isEmpty) {
      throw new RuntimeException(
        "The input model does not have a SQL Transformer, so it cannot be scored against a DB table."
      )
    }

    verifyColumns(inputModel, inputDataset)

    val dependentColumn = inputModel.dependentFeature
    val unitModel = new UnitModel(Seq(dependentColumn))
    val combinedModel = CombinerModel.make(Seq(unitModel, inputModel))
    val classificationSQL = combinedModel.sqlTransformer(sqlGenerator).get.getSQL

    val quotedFullTableName: String = getQuotedSchemaTableName(inputDataset.schemaName, inputDataset.tableName)

    val aliasGenerator: AliasGenerator = new AliasGenerator

    val innerSelectStatement = SQLUtility.getSelectStatement(
      sql = classificationSQL,
      inputTableName = quotedFullTableName,
      aliasGenerator = aliasGenerator,
      sqlGenerator = sqlGenerator
    )
    val predictedColumnName: ColumnName = classificationSQL.layers.last.last._2

    val observedColumnNameEscaped: String = sqlGenerator.escapeColumnName(dependentColumn.columnName)
    val predictedColumnNameEscaped: String = predictedColumnName.escape(sqlGenerator)
    val selectSqlStatement =
      s"""SELECT
          | $observedColumnNameEscaped AS ${sqlGenerator.escapeColumnName(ConfusionMatrixUtils.observedColumnName)},
          | $predictedColumnNameEscaped AS ${sqlGenerator.escapeColumnName(ConfusionMatrixUtils.predictedColumnName)},
          | COUNT(*) AS "N"
          | FROM ($innerSelectStatement) AS ${aliasGenerator.getNextAlias}
          | GROUP BY $observedColumnNameEscaped, $predictedColumnNameEscaped""".stripMargin

    val sqlStatementBuilder = new StringBuilder()
    if (isView) {
      sqlStatementBuilder ++= "CREATE VIEW " + fullOutputName + " AS ("
    } else {
      sqlStatementBuilder ++= "CREATE TABLE " + fullOutputName + " AS ("
    }
    sqlStatementBuilder ++= selectSqlStatement + ");"
    sqlStatementBuilder.toString
  }

  /**
    * Verifies that the columns needed for the model evaluation are present in the input dataset.
    * Throws an exception otherwise.
    */
  @throws(classOf[RuntimeException])
  def verifyColumns(inputModel: ClassificationRowModel, inputDataset: DBTable): Unit = {
    val datasetColumnNames = inputDataset.tabularSchema.definedColumns.map(_.columnName).toSet
    val dependentColumnName = inputModel.dependentFeature.columnName
    val missingDependentColumn: List[String] = {
      if (datasetColumnNames.contains(dependentColumnName)) {
        Nil
      } else {
        List(dependentColumnName)
      }
    }
    val missingIndependentColumns: Seq[String] = inputModel.inputFeatures.map(_.columnName).filterNot(name => datasetColumnNames.contains(name))
    val missingColumns = missingIndependentColumns ++ missingDependentColumn
    if (missingColumns.nonEmpty) {
      throw new RuntimeException(
        "The input dataset is missing the columns " + missingColumns + " needed for evaluating the model."
      )
    }
  }

  def getQuotedSchemaTableName(schemaName: String, tableName: String): String = {
    quoteName(schemaName) + "." + quoteName(tableName)
  }

  def quoteName(colName: String): String = {
    "\"" + colName + "\""
  }

}
