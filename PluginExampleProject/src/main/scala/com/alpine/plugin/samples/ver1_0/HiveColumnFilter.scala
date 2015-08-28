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

import scala.collection.mutable

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HiveTableDefault
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.HiveParameterUtils
import com.alpine.plugin.core.{OperatorGUINode, OperatorListener, OperatorMetadata, OperatorParameters, OperatorSignature}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
/**
 * This is an example operator that demonstrates how to:
 * -- take a HiveTable as input
 * -- use HQL to generate a new HiveTable
 * -- pass a HiveTable as output
 */
class HiveColumnFilterSignature extends OperatorSignature[
  HiveColumnFilterGUINode,
  HiveColumnFilterRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Hive Column Filter",
      category = "Transformation",
      author = "Jenny Thompson",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class HiveColumnFilterGUINode extends OperatorGUINode[HiveTable, HiveTable] {
  override def onPlacement(
                            operatorDialog: OperatorDialog,
                            operatorDataSourceManager: OperatorDataSourceManager,
                            operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addTabularDatasetColumnCheckboxes(
      "columnsToKeep",
      "Columns to Keep",
      ColumnFilter.All,
      "main"
    )

    HiveParameterUtils.addStandardOutputParameters(operatorDialog)
  }

  private def updateOutputSchema(inputSchemas: mutable.Map[String, TabularSchema],
                                  params: OperatorParameters,
                                  operatorSchemaManager: OperatorSchemaManager): Unit = {
    // There can only be one input schema.
    if (inputSchemas.nonEmpty) {
      val inputSchema = inputSchemas.values.iterator.next()
      if (inputSchema.getDefinedColumns().nonEmpty) {

        val (_, columnsToKeepArray) =
          params.getTabularDatasetSelectedColumns("columnsToKeep")
        val columnsToKeep = columnsToKeepArray.toSet
        val columnDefs: Seq[ColumnDef] = inputSchema.getDefinedColumns()
          .filter(colDef => columnsToKeep.contains(colDef.columnName))
        val outputSchema = TabularSchema(columnDefs,TabularFormatAttributes.createHiveFormat())
        operatorSchemaManager.setOutputSchema(outputSchema)
      }
    }
  }

  override def onInputOrParameterChange(inputSchemas: mutable.Map[String, TabularSchema],
                                         params: OperatorParameters,
                                         operatorSchemaManager: OperatorSchemaManager): Unit = {
    this.updateOutputSchema(inputSchemas, params, operatorSchemaManager)
  }

}

class HiveColumnFilterRuntime extends SparkRuntimeWithIOTypedJob[
  HiveColumnFilterJob,
  HiveTable,
  HiveTable]

class HiveColumnFilterJob extends SparkIOTypedPluginJob[HiveTable, HiveTable] {
  override def onExecution(
    sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: HiveTable,
    operatorParameters: OperatorParameters,
    listener: OperatorListener): HiveTable = {

    val sparkUtils = new SparkRuntimeUtils(sparkContext)

    listener.notifyMessage("Starting the column filter.")

    val (_, columnsToKeep) = operatorParameters.getTabularDatasetSelectedColumns("columnsToKeep")

    listener.notifyMessage("Columns to keep are : " + columnsToKeep.mkString(", "))

    val hiveContext = new HiveContext(sparkContext)

    val outputTableName = HiveParameterUtils.getResultTableName(operatorParameters)
    val outputDBName = HiveParameterUtils.getResultDBName(operatorParameters)
    val fullOutputName = HiveTable.getConcatenatedName(outputTableName, outputDBName)

    listener.notifyMessage("Full output name is : " + fullOutputName)

    val overwrite = HiveParameterUtils.getOverwriteParameterValue(operatorParameters)
    if (overwrite) {
      executeSQL(listener, hiveContext, s"""DROP TABLE IF EXISTS $fullOutputName""")
    }

    val sql = s"""CREATE TABLE $fullOutputName AS SELECT ${columnsToKeep.mkString(", ")} FROM ${input.getConcatenatedName}"""
    executeSQL(listener, hiveContext, sql)

    new HiveTableDefault(
      outputTableName,
      outputDBName,
      sparkUtils.convertSparkSQLSchemaToTabularSchema(hiveContext.table(fullOutputName).schema)
    )
  }

  def executeSQL(listener: OperatorListener, hiveContext: HiveContext, sql: String): Unit = {
    listener.notifyMessage("SQL to execute: " + sql)
    hiveContext.sql(sql)
  }
}

