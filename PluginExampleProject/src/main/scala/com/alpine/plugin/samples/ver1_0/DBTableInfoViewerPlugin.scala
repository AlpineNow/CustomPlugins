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
import com.alpine.plugin.core.db.{DBExecutionContext, DBRuntime}
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.IOStringDefault

class DBTableInfoViewerSignature extends OperatorSignature[
  DBTableInfoViewerGUINode,
  DBTableInfoViewerRuntime] {
  def getMetadata(): OperatorMetadata = {
    new OperatorMetadata(
      name = "Sample - DB Table Info Viewer",
      category = "Plugin Sample - DB",
      author = "Sung Chung",
      version = 1,
      helpURL = "",
      iconNamePrefix = ""
    )
  }
}

class DBTableInfoViewerGUINode extends OperatorGUINode[
  IONone,
  IOString] {
  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {
    operatorDialog.addDataSourceDropdownBox(
      id = "dbName",
      label = "Database",
      dataSourceManager = operatorDataSourceManager
    )

    operatorDialog.addDBSchemaDropdownBox(
      id = "dbSchema",
      label = "Database Schema",
      defaultSchema = ""
    )

    operatorDialog.addDBTableDropdownBox(
      id = "dbTable",
      label = "Database Table",
      defaultTable = ""
    )
  }
}

class DBTableInfoViewerRuntime extends DBRuntime[IONone, IOString] {
  override def onExecution(context: DBExecutionContext,
                           input: IONone,
                           params: OperatorParameters,
                           listener: OperatorListener): IOString = {
    val dbName = params.getStringValue("dbName")
    val dbSchema = params.getStringValue("dbSchema")
    val dbTable = params.getStringValue("dbTable")

    val outputBuilder = new StringBuilder
    outputBuilder.append("Database : " + dbName + "\n")
    outputBuilder.append("Schema : " + dbSchema + "\n")
    outputBuilder.append("Table : " + dbTable + "\n")
    val connectionInfo = context.getDBConnectionInfo
    val getColumnNameStmtBuilder = new StringBuilder
    getColumnNameStmtBuilder.append(
      "SELECT column_name, data_type FROM information_schema.columns "
    ).append(
      "WHERE table_schema = '" + dbSchema + "' "
    ).append(
      "AND table_name = '" + dbTable + "'"
    )

    val stmt = connectionInfo.connection.createStatement()
    val results = stmt.executeQuery(getColumnNameStmtBuilder.toString())

    while (results.next()) {
      outputBuilder.
        append("ColumnName : ").
        append(results.getString("column_name") + ", ").
        append("ColumnType : ").
        append(results.getString("data_type")).
        append("\n")
    }

    new IOStringDefault(outputBuilder.toString(), Some(params.operatorInfo))
  }
}