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
import com.alpine.sql.{AliasGenerator, DatabaseType, SQLExecutor, SQLGenerator}
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

  override def getMetadata: OperatorMetadata = new OperatorMetadata(
    name        = "Sample - DB Confusion Matrix",
    category    = "Plugin Sample - DB",
    author      = Some("Alpine Data"),
    version     = 1,
    helpURL     = None,
    icon        = None,
    toolTipText = Some("Enter text to show as a tooltip for your operator here. " +
      "This will appear when a user hovers over the operator’s name in the workflow editor. " +
      "The best tooltips concisely describe the function of the operator and are no more than fifty words.")
  )
}

class DBConfusionMatrixGUINode extends OperatorGUINode[
  Tuple2[ClassificationModelWrapper, DBTable],
  DBTable] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    //add parameters to let the user determine how the output table will be written
    DBParameterUtils.addStandardDBOutputParameters(operatorDialog)
    operatorSchemaManager.setOutputSchema(ConfusionMatrixUtils.outputTabularSchema)
  }

}

object ConfusionMatrixUtils {

  val observedColumnName:  String = "Observed"
  val predictedColumnName: String = "Predicted"
  val countColumnName:     String = "N" // Don't use "count" as it is a SQL keyword.

  val outputTabularSchema = TabularSchema(
    Seq(
      ColumnDef(observedColumnName,  ColumnType.String),
      ColumnDef(predictedColumnName, ColumnType.String),
      ColumnDef(countColumnName,     ColumnType.String)
    )
  )

}

class DBConfusionMatrixRuntime extends DBRuntime[Tuple2[ClassificationModelWrapper, DBTable], DBTable] {

  override def onExecution(
      context: DBExecutionContext,
      input: Tuple2[ClassificationModelWrapper, DBTable],
      params: OperatorParameters,
      listener: OperatorListener
  ): DBTable = {
    //get the output parameters
    val outputSchema   = DBParameterUtils.getDBOutputSchemaParam(params)
    val isView         = DBParameterUtils.getIsViewParam(params)
    val outputName     = DBParameterUtils.getResultTableName(params)
    val sqlExecutor    = context.getSQLExecutor

    //check if there is a table or view with the same name as the output table and drop according to the
    // "dropIfExist" field
    val dropIfExists = DBParameterUtils.getDropIfExistsParameterValue(params)
    val fullOutputName = getQuotedSchemaTableName(outputSchema, outputName)
    if (dropIfExists) {
      try {
        // This will throw an exception if there is a view with the output name,
        // we will catch the exception and delete the view in the next block of code.
        listener.notifyMessage("Dropping table or view " + fullOutputName + " if it exists")
        sqlExecutor.ddlDropTableOrViewIfExists(fullOutputName, cascadeFlag = true)
      }
      catch {
        case (e: Exception) => listener.notifyMessage("Could not drop table " + fullOutputName);
      }
    }

    val outputSQL = getCreateTableSQL(sqlExecutor.getSQLGenerator, input, isView, fullOutputName)

    sqlExecutor.executeUpdate(outputSQL)

    DBTableDefault(
      outputSchema,
      outputName,
      ConfusionMatrixUtils.outputTabularSchema
    )
  }

  def getCreateTableSQL(
      sqlGenerator: SQLGenerator,
      input: Tuple2[_ <: ClassificationModelWrapper, _ <: DBTable],
      isView: Boolean,
      fullOutputName: String
  ): String = {
    val inputDataset: DBTable                = input._2
    val inputModel:   ClassificationRowModel = input._1.model

    val sqlTransformerOption                 = inputModel.sqlTransformer(sqlGenerator)

    if (sqlTransformerOption.isEmpty) {
      throw new RuntimeException(
        "The input model does not have a SQL Transformer, so it cannot be scored against a DB table."
      )
    }

    verifyColumns(inputModel, inputDataset)

    val dependentColumn   = inputModel.dependentFeature
    val unitModel         = UnitModel(Seq(dependentColumn))
    val combinedModel     = CombinerModel.make(Seq(unitModel, inputModel))
    val classificationSQL = combinedModel.sqlTransformer(sqlGenerator).get.getSQL

    val quotedFullTableName: String = getQuotedSchemaTableName(inputDataset.schemaName, inputDataset.tableName)

    val aliasGenerator: AliasGenerator = new AliasGenerator

    val innerSelectStatement = SQLUtility.getSelectStatement(
      sql            = classificationSQL,
      inputTableName = quotedFullTableName,
      aliasGenerator = aliasGenerator,
      sqlGenerator   = sqlGenerator
    )
    val predictedColumnName: ColumnName = classificationSQL.layers.last.last._2

    val observedColumnNameEscaped  = sqlGenerator.quoteIdentifier(dependentColumn.columnName)
    val predictedColumnNameEscaped = predictedColumnName.escape(sqlGenerator)
    val selectClause = s"""
          | $observedColumnNameEscaped AS ${sqlGenerator.quoteIdentifier(ConfusionMatrixUtils.observedColumnName)},
          | $predictedColumnNameEscaped AS ${sqlGenerator.quoteIdentifier(ConfusionMatrixUtils.predictedColumnName)},
          | COUNT(*) AS ${sqlGenerator.quoteIdentifier(ConfusionMatrixUtils.countColumnName)}
          | """.stripMargin
    val aliasSubqueries = sqlGenerator.useAliasForSelectSubQueries
    val sourceTable = {
      if (aliasSubqueries) {
        s"""
           | ($innerSelectStatement) AS ${aliasGenerator.getNextAlias}
           | """.stripMargin
      } else {
        s"""
           | ($innerSelectStatement)
           | """.stripMargin
      }
    }
    val groupByClause = s"""GROUP BY $observedColumnNameEscaped, $predictedColumnNameEscaped"""
    sqlGenerator
        .getCreateTableOrViewAsSelectSQL(
          columns = selectClause,
          sourceTable = sourceTable,
          destinationTable = fullOutputName,
          whereClause = groupByClause,
          isView = isView
        )
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
