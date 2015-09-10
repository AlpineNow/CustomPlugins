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

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob, SparkDataFrameRuntime}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import org.apache.spark.sql.DataFrame

class ColumnFilterSignature extends OperatorSignature[
  ColumnFilterGUINode,
  ColumnFilterRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - Spark ColumnFilter",
      category = "Plugin Sample - Spark",
      author = "Egor Pakhomov",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

object ColumnFilterUtil {
  val COLUMNS_TO_KEEP_KEY = "columnsToKeep"
  def getColumnsToKeep(parameters: OperatorParameters): Seq[String] = {
    parameters.getTabularDatasetSelectedColumns(ColumnFilterUtil.COLUMNS_TO_KEEP_KEY)._2
  }
}

class ColumnFilterGUINode extends SparkDataFrameGUINode[ColumnFilterJob] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addTabularDatasetColumnCheckboxes(
      ColumnFilterUtil.COLUMNS_TO_KEEP_KEY,
      "Columns to keep",
      ColumnFilter.All,
      "main"
    )

    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

  override def defineOutputSchemaColumns(inputSchema: TabularSchema,
                                         parameters: OperatorParameters): Seq[ColumnDef] = {
    val columnsToKeep = ColumnFilterUtil.getColumnsToKeep(parameters).toSet
    inputSchema.getDefinedColumns.filter(colDef => columnsToKeep.contains(colDef.columnName))
  }

}

class ColumnFilterRuntime extends SparkDataFrameRuntime[ColumnFilterJob] {}

class ColumnFilterJob extends SparkDataFrameJob {
  override def transform(parameters: OperatorParameters,
                         dataFrame: DataFrame,
                         sparkUtils: SparkRuntimeUtils,
                         listener: OperatorListener): DataFrame = {
    val columnNamesToKeep = ColumnFilterUtil.getColumnsToKeep(parameters)
    val columnsToKeep = columnNamesToKeep.map(dataFrame.col)
    dataFrame.select(columnsToKeep: _*)
  }
}
